use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex, OnceLock};
use std::thread::sleep;
use std::time::{Duration, Instant};

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, LogicalObjectId,
    MetadataBackend, NamespaceScope, ObjectKey, ObjectRoute, ReplicaRoute, RouteCasRequest,
    RouteDirectory, RoutePolicy, RoutePolicyDomain, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, SegmentTargetChunk, StoreError,
    TenantBandwidthShapingPolicy, TenantExecutionFairnessPolicy, TenantObjectAccounting,
    TenantPlacementPolicy, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
    TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome, TenantQuotaPolicy, TenantQuotaReservation,
    TenantQuotaReservationOutcome, TenantQuotaState, TenantRoutePolicy,
};
use mooncake_transport::{Opcode, TransferPacingMode};
use parking_lot::Mutex;

use super::{
    align_up_u64, bootstrap_route_policy, cached_live_client_snapshot, compatibility_matches,
    control_bind_host, copy_into_region, effective_route_policy, encode_lifecycle_state,
    flatten_slices, now_ms, payload_checksum, record_success_metric, route_topk_from_tenant_spec,
    scatter_into_buffers, shared_suspect_runtime_cache, stable_debug_log_sample,
    startup_prewarm_delay, AllocationSpan, LiveClientCache, LocalAllocatorAdapter,
    LocalAllocatorState, LocalAuthorityAdapter, PendingReclaim, ReplicaWriteTarget, ResolvedObject,
    SegmentAllocator, StorageOwnerState, StoreState, SuspectRuntimeCache,
};
use crate::{
    control_plane::{
        control_address_label, AllocatorService, ControlPlaneClient, ControlPlaneHandle, ReleaseOp,
    },
    memory::{with_test_numa_locations, RegionAllocation},
    metrics_test_lock, render_prometheus_metrics, reset_metrics,
    route_directory::{authority_get, authority_replace, build_route_directory},
    snapshot_metrics, BandwidthShaping, ExecutionFairness, GetRequest, LocalMemoryConfig,
    MooncakeCompatibilityFacade, MultiBufferGetRequest, MultiBufferPutRequest, NamespaceQuota,
    ObjectRef, PlacementPlanner, PutFromRequest, PutRequest, ReplicationPolicy, RouteControlMode,
    StoreClient, StoreClientBuilder,
};
use mooncake_store_route::RouteAuthorityService;

use mooncake_store_test_utils::transport::TestTransport;

fn test_lifecycle_state(state: ClientLifecycleState) -> Arc<AtomicU8> {
    Arc::new(AtomicU8::new(encode_lifecycle_state(state)))
}

fn test_route_write_gate() -> Arc<Mutex<()>> {
    Arc::new(Mutex::new(()))
}

struct NoHotPathMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    deny_tenant_policy_list: bool,
    deny_live_client_list: bool,
    deny_segment_list: bool,
}

struct HotPathBlockedMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    block_hot_path: AtomicBool,
}

impl NoHotPathMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self {
            inner,
            deny_tenant_policy_list: false,
            deny_live_client_list: false,
            deny_segment_list: false,
        }
    }

    fn with_tenant_policy_list_blocked(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self {
            inner,
            deny_tenant_policy_list: true,
            deny_live_client_list: false,
            deny_segment_list: false,
        }
    }
}

impl HotPathBlockedMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self {
            inner,
            block_hot_path: AtomicBool::new(false),
        }
    }

    fn block_hot_path(&self) {
        self.block_hot_path.store(true, Ordering::Relaxed);
    }

    fn hot_path_error(&self, op: &str) -> mooncake_store_core::Result<()> {
        if self.block_hot_path.load(Ordering::Relaxed) {
            return Err(StoreError::Unsupported(format!(
                "metadata hot path is disabled in this test: {op}"
            )));
        }
        Ok(())
    }
}

struct CountingMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    list_live_clients_calls: AtomicUsize,
    get_object_route_calls: AtomicUsize,
    upsert_client_lease_calls: AtomicUsize,
    update_client_state_calls: AtomicUsize,
    reject_state_patch: bool,
    filter_expired_live_clients: bool,
}

struct CountingRouteDirectory {
    inner: Arc<dyn RouteDirectory>,
    get_object_routes_calls: AtomicUsize,
}

struct FinalizeFailureMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    fail_reservation_id: String,
}

struct StaticAllocatorAdapter {
    runtime: ClientRuntimeId,
    allocator: Arc<Mutex<LocalAllocatorState>>,
}

impl AllocatorService for StaticAllocatorAdapter {
    fn reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
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
    ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
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
    ) -> mooncake_store_core::Result<()> {
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
}

#[derive(Default)]
struct NoopEvictionService;

impl crate::control_plane::EvictionService for NoopEvictionService {
    fn batch_report_route_hits(
        &self,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<crate::control_plane::RouteTrafficReport> {
        Ok(crate::control_plane::RouteTrafficReport::new(keys.len(), 0))
    }

    fn batch_track_routes(
        &self,
        routes: &[ObjectRoute],
    ) -> mooncake_store_core::Result<crate::control_plane::RouteTrafficReport> {
        Ok(crate::control_plane::RouteTrafficReport::new(
            routes.len(),
            0,
        ))
    }
}

struct BlockingCasMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    block_key: ObjectKey,
    gate: Arc<(StdMutex<BlockingCasGateState>, Condvar)>,
}

struct RecoverableMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    state: StdMutex<RecoverableMetadataState>,
}

#[derive(Default)]
struct BlockingCasGateState {
    armed: bool,
    entered: bool,
    blocked_count: usize,
    released: bool,
    blocked_once: bool,
}

#[derive(Default)]
struct RecoverableMetadataState {
    lease_failures_remaining: usize,
    hidden_clients: BTreeSet<String>,
    hidden_segments: BTreeSet<String>,
    hide_route_policy: bool,
}

impl CountingMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self::with_options(inner, false, false)
    }

    fn with_reject_state_patch(
        inner: Arc<InMemoryMetadataBackend>,
        reject_state_patch: bool,
    ) -> Self {
        Self::with_options(inner, reject_state_patch, false)
    }

    fn with_expiring_live_clients(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self::with_options(inner, false, true)
    }

    fn with_options(
        inner: Arc<InMemoryMetadataBackend>,
        reject_state_patch: bool,
        filter_expired_live_clients: bool,
    ) -> Self {
        Self {
            inner,
            list_live_clients_calls: AtomicUsize::new(0),
            get_object_route_calls: AtomicUsize::new(0),
            upsert_client_lease_calls: AtomicUsize::new(0),
            update_client_state_calls: AtomicUsize::new(0),
            reject_state_patch,
            filter_expired_live_clients,
        }
    }

    fn list_live_clients_calls(&self) -> usize {
        self.list_live_clients_calls.load(Ordering::Relaxed)
    }

    fn get_object_route_calls(&self) -> usize {
        self.get_object_route_calls.load(Ordering::Relaxed)
    }

    fn upsert_client_lease_calls(&self) -> usize {
        self.upsert_client_lease_calls.load(Ordering::Relaxed)
    }

    fn update_client_state_calls(&self) -> usize {
        self.update_client_state_calls.load(Ordering::Relaxed)
    }

    fn visible_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
        let leases = self.inner.list_live_clients()?;
        if !self.filter_expired_live_clients {
            return Ok(leases);
        }
        let now = now_ms();
        Ok(leases
            .into_iter()
            .filter(|lease| lease.expires_at_ms >= now)
            .collect())
    }
}

impl CountingRouteDirectory {
    fn new(inner: Arc<dyn RouteDirectory>) -> Self {
        Self {
            inner,
            get_object_routes_calls: AtomicUsize::new(0),
        }
    }

    fn get_object_routes_calls(&self) -> usize {
        self.get_object_routes_calls.load(Ordering::Relaxed)
    }
}

impl RecoverableMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self {
            inner,
            state: StdMutex::new(RecoverableMetadataState::default()),
        }
    }

    fn fail_next_lease_upserts(&self, count: usize) {
        self.state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .lease_failures_remaining = count;
    }

    fn hide_runtime_metadata(&self, runtime: &ClientRuntimeId) {
        let segments = self
            .inner
            .list_segments(Some(runtime))
            .expect("segments should list before hiding");
        let mut state = self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed");
        state.hidden_clients.insert(runtime.storage_key());
        for segment in segments {
            state
                .hidden_segments
                .insert(Self::segment_key(&segment.owner, &segment.segment_name));
        }
    }

    fn hide_route_policy(&self) {
        self.state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hide_route_policy = true;
    }

    fn segment_key(owner: &ClientRuntimeId, segment: &SegmentName) -> String {
        format!("{}:{}", owner.storage_key(), segment.0)
    }
}

impl FinalizeFailureMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>, fail_reservation_id: impl Into<String>) -> Self {
        Self {
            inner,
            fail_reservation_id: fail_reservation_id.into(),
        }
    }
}

impl BlockingCasMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>, block_key: &str) -> Self {
        Self {
            inner,
            block_key: ObjectKey::new(block_key),
            gate: Arc::new((
                StdMutex::new(BlockingCasGateState {
                    armed: false,
                    entered: false,
                    blocked_count: 0,
                    released: false,
                    blocked_once: false,
                }),
                Condvar::new(),
            )),
        }
    }

    fn arm_blocked_cas(&self) {
        let (lock, _) = &*self.gate;
        let mut state = lock.lock().expect("blocking CAS gate lock should succeed");
        assert!(
            !state.entered || state.released,
            "cannot arm blocked CAS while a previous CAS is still blocked"
        );
        state.armed = true;
        state.entered = false;
        state.blocked_count = 0;
        state.released = false;
        state.blocked_once = false;
    }

    fn wait_until_blocked(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (lock, condvar) = &*self.gate;
        let mut state = lock.lock().expect("blocking CAS gate lock should succeed");
        while !state.entered {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let wait_for = deadline.saturating_duration_since(now);
            let (next, result) = condvar
                .wait_timeout(state, wait_for)
                .expect("blocking CAS gate wait should succeed");
            state = next;
            if result.timed_out() && !state.entered {
                return false;
            }
        }
        true
    }

    fn wait_until_blocked_count(&self, count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (lock, condvar) = &*self.gate;
        let mut state = lock.lock().expect("blocking CAS gate lock should succeed");
        while state.blocked_count < count {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let wait_for = deadline.saturating_duration_since(now);
            let (next, result) = condvar
                .wait_timeout(state, wait_for)
                .expect("blocking CAS gate wait should succeed");
            state = next;
            if result.timed_out() && state.blocked_count < count {
                return false;
            }
        }
        true
    }

    fn release_blocked_cas(&self) {
        let (lock, condvar) = &*self.gate;
        let mut state = lock.lock().expect("blocking CAS gate lock should succeed");
        state.released = true;
        condvar.notify_all();
    }
}

impl MetadataBackend for RecoverableMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
        {
            let mut state = self
                .state
                .lock()
                .expect("recoverable metadata state lock should succeed");
            if state.lease_failures_remaining > 0 {
                state.lease_failures_remaining -= 1;
                return Err(StoreError::Metadata("simulated redis outage".to_string()));
            }
            state.hidden_clients.remove(&lease.runtime.storage_key());
        }
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
        let hidden = self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hidden_clients
            .clone();
        let lease = self.inner.get_client_lease(runtime)?;
        Ok(lease.filter(|lease| !hidden.contains(&lease.runtime.storage_key())))
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> mooncake_store_core::Result<Option<ClientLease>> {
        let hidden = self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hidden_clients
            .clone();
        let lease = self.inner.get_live_runtime_by_stable_id(stable_id)?;
        Ok(lease.filter(|lease| !hidden.contains(&lease.runtime.storage_key())))
    }

    fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
        let hidden = self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hidden_clients
            .clone();
        Ok(self
            .inner
            .list_live_clients()?
            .into_iter()
            .filter(|lease| !hidden.contains(&lease.runtime.storage_key()))
            .collect())
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> mooncake_store_core::Result<()> {
        self.state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hidden_segments
            .remove(&Self::segment_key(&segment.owner, &segment.segment_name));
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
        let hidden = self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hidden_segments
            .clone();
        let segment = self.inner.get_segment(owner, segment)?;
        Ok(segment.filter(|segment| {
            !hidden.contains(&Self::segment_key(&segment.owner, &segment.segment_name))
        }))
    }

    fn list_segments(
        &self,
        owner: Option<&ClientRuntimeId>,
    ) -> mooncake_store_core::Result<Vec<SegmentAnnouncement>> {
        let hidden = self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hidden_segments
            .clone();
        Ok(self
            .inner
            .list_segments(owner)?
            .into_iter()
            .filter(|segment| {
                !hidden.contains(&Self::segment_key(&segment.owner, &segment.segment_name))
            })
            .collect())
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
        if self
            .state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hide_route_policy
        {
            return Ok(None);
        }
        self.inner.get_route_policy(domain)
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> mooncake_store_core::Result<bool> {
        self.state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hide_route_policy = false;
        self.inner.put_route_policy_if_absent(domain, policy)
    }

    fn put_route_policy(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> mooncake_store_core::Result<()> {
        self.state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .hide_route_policy = false;
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
        request: &mooncake_store_core::TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &mooncake_store_core::TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
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

impl MetadataBackend for NoHotPathMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
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
        if self.deny_live_client_list {
            return Err(StoreError::Unsupported(
                "live client listing is disabled in this test".to_string(),
            ));
        }
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
        if self.deny_segment_list {
            return Err(StoreError::Unsupported(
                "segment listing is disabled in this test".to_string(),
            ));
        }
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
        Err(StoreError::Unsupported(
            "metadata route listing hot path is disabled in this test".to_string(),
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
        if self.deny_tenant_policy_list {
            return Err(StoreError::Unsupported(
                "tenant policy listing is disabled in this test".to_string(),
            ));
        }
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
        request: &mooncake_store_core::TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &mooncake_store_core::TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
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

impl MetadataBackend for HotPathBlockedMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
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

    fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
        self.hot_path_error("list_live_clients")?;
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

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> mooncake_store_core::Result<Option<SegmentAnnouncement>> {
        self.hot_path_error("get_segment")?;
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
    ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
        self.hot_path_error("reserve_segment")?;
        self.inner.reserve_segment(owner, segment, length_bytes)
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<()> {
        self.hot_path_error("release_segment")?;
        self.inner
            .release_segment(owner, segment, offset_bytes, length_bytes)
    }

    fn get_object_route(
        &self,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<mooncake_store_core::ObjectRoute>> {
        self.hot_path_error("get_object_route")?;
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
        self.hot_path_error("compare_and_swap_object_route")?;
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
        self.hot_path_error("get_tenant_policy")?;
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
        self.hot_path_error("get_tenant_object_accounting")?;
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
        request: &mooncake_store_core::TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.hot_path_error("reserve_tenant_quota")?;
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &mooncake_store_core::TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.hot_path_error("finalize_tenant_quota")?;
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.hot_path_error("abort_tenant_quota")?;
        self.inner.abort_tenant_quota(reservation_id)
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
        self.upsert_client_lease_calls
            .fetch_add(1, Ordering::Relaxed);
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
        self.update_client_state_calls
            .fetch_add(1, Ordering::Relaxed);
        if self.reject_state_patch {
            return Err(StoreError::NotFound(runtime.storage_key()));
        }
        if self.filter_expired_live_clients
            && !self
                .visible_live_clients()?
                .into_iter()
                .any(|lease| lease.runtime == *runtime)
        {
            return Err(StoreError::NotFound(runtime.storage_key()));
        }
        self.inner.update_client_state(runtime, next)
    }

    fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
        self.list_live_clients_calls.fetch_add(1, Ordering::Relaxed);
        self.visible_live_clients()
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
        self.get_object_route_calls.fetch_add(1, Ordering::Relaxed);
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
        request: &mooncake_store_core::TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &mooncake_store_core::TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
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

impl RouteDirectory for CountingRouteDirectory {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.inner.get_object_route(observer, key)
    }

    fn get_object_routes(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<Vec<Option<ObjectRoute>>> {
        self.get_object_routes_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_object_routes(observer, keys)
    }

    fn get_object_routes_bounded(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<Vec<Option<ObjectRoute>>> {
        self.inner.get_object_routes_bounded(observer, keys)
    }

    fn contains_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.contains_object_route(observer, key)
    }

    fn contains_object_routes_bounded(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<Vec<bool>> {
        self.inner.contains_object_routes_bounded(observer, keys)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<mooncake_store_core::CasResult>>>
    {
        self.inner
            .compare_and_swap_object_routes(observer, requests)
    }

    fn list_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.inner.list_routes_by_replica_owner(observer, owner)
    }

    fn list_routes_in_scope(
        &self,
        observer: &ClientLease,
        scope: &NamespaceScope,
    ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.inner.list_routes_in_scope(observer, scope)
    }

    fn list_reuse_candidates(
        &self,
        observer: &ClientLease,
        reuse: &mooncake_store_core::ReuseIdentity,
    ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.inner.list_reuse_candidates(observer, reuse)
    }

    fn get_version_floor(&self, observer: &ClientLease, key: &ObjectKey) -> Option<RouteVersion> {
        self.inner.get_version_floor(observer, key)
    }

    fn get_version_floors(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Vec<Option<RouteVersion>> {
        self.inner.get_version_floors(observer, keys)
    }
}

impl MetadataBackend for FinalizeFailureMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
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
        request: &mooncake_store_core::TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &mooncake_store_core::TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        if request.reservation_id == self.fail_reservation_id {
            return Err(StoreError::InvalidState(format!(
                "injected finalize failure for {}",
                request.reservation_id
            )));
        }
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
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

impl MetadataBackend for BlockingCasMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
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
        if *key == self.block_key {
            let (lock, condvar) = &*self.gate;
            let mut state = lock.lock().expect("blocking CAS gate lock should succeed");
            if state.armed && !state.blocked_once {
                state.entered = true;
                state.blocked_count += 1;
                condvar.notify_all();
                while !state.released {
                    state = condvar
                        .wait(state)
                        .expect("blocking CAS gate wait should succeed");
                }
                state.blocked_once = true;
                state.armed = false;
            }
        }
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
        request: &mooncake_store_core::TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &mooncake_store_core::TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
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

fn storage_config() -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(4096)
        .scratch_bytes(4096)
        .reclaim_grace_ms(0)
        .eviction_poll_interval(Duration::ZERO)
}

fn storage_config_with_bytes(storage_bytes: usize) -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(storage_bytes)
        .scratch_bytes(4096)
        .alignment(1)
        .reclaim_grace_ms(0)
        .eviction_poll_interval(Duration::ZERO)
}

fn storage_config_with_background_eviction(
    storage_bytes: usize,
    high_percent: u8,
    low_percent: u8,
) -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
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
        .numa_aware(false)
        .storage_bytes(storage_bytes)
        .scratch_bytes(scratch_bytes)
        .alignment(alignment)
        .reclaim_grace_ms(0)
        .eviction_poll_interval(Duration::ZERO)
}

fn rw_only_config() -> LocalMemoryConfig {
    storage_config_with_layout(0, 4096, 1)
}

fn fast_live_client_sync_interval() -> Duration {
    Duration::from_millis(25)
}

fn resolved_object_for_route(
    reader: &StoreClient,
    tenant: &str,
    key: &str,
    route: ObjectRoute,
) -> ResolvedObject {
    let local_segments = reader.local_storage_segments();
    let readable_runtimes = reader
        .readable_runtime_set(true)
        .expect("readable runtime refresh should succeed");
    let replica = StoreClient::select_readable_replica(
        &route,
        reader.runtime_id(),
        &local_segments,
        &readable_runtimes,
    )
    .expect("route should expose a readable replica");
    let fallback_replicas = StoreClient::fallback_replicas_for_route(
        &route,
        &replica,
        reader.runtime_id(),
        &local_segments,
        &readable_runtimes,
    );
    ResolvedObject {
        tenant: tenant.to_string(),
        key: key.to_string(),
        route,
        replica,
        fallback_replicas,
    }
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
        route_directory,
        allocator,
    ))
}

fn publish_storage_node(
    metadata: &Arc<InMemoryMetadataBackend>,
    transport: &Arc<TestTransport>,
    stable_id: &str,
    segment_name: &str,
    pool: &str,
) -> ClientRuntimeId {
    publish_storage_node_with_capacity(metadata, transport, stable_id, segment_name, pool, 4096, 64)
}

fn publish_storage_node_with_capacity(
    metadata: &Arc<InMemoryMetadataBackend>,
    transport: &Arc<TestTransport>,
    stable_id: &str,
    segment_name: &str,
    pool: &str,
    capacity_bytes: u64,
    alignment_bytes: u64,
) -> ClientRuntimeId {
    let storage_transport = Arc::new(transport.peer(segment_name));
    let storage = Arc::new(
        StoreClientBuilder::new(metadata.clone(), stable_id)
            .state(ClientLifecycleState::Active)
            .label("pool", pool)
            .label("storage", "true")
            .segment_name(segment_name)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(storage_transport)
            .local_memory(storage_config_with_layout(
                capacity_bytes as usize,
                4096,
                alignment_bytes as usize,
            ))
            .build(test_future_expiry_ms())
            .expect("storage client build should succeed"),
    );
    storage
        .register_local_memory()
        .expect("storage memory should register");
    let runtime = storage.runtime_id().clone();
    metadata
        .upsert_client_lease(&storage.lease())
        .expect("storage lease should upsert");
    test_storage_nodes().lock().push(storage);
    runtime
}

#[allow(clippy::too_many_arguments)]
fn publish_labeled_storage_node_with_capacity(
    metadata: &Arc<InMemoryMetadataBackend>,
    transport: &TestTransport,
    stable_id: &str,
    segment_name: &str,
    pool: &str,
    capacity_bytes: u64,
    alignment_bytes: u64,
    domain: Option<&str>,
    object_set: Option<&str>,
    qos_tier: Option<&str>,
) -> ClientRuntimeId {
    let storage_transport = Arc::new(transport.peer(segment_name));
    let mut builder = StoreClientBuilder::new(metadata.clone(), stable_id)
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .label("pool", pool)
        .label("storage", "true")
        .segment_name(segment_name)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config_with_layout(
            capacity_bytes as usize,
            4096,
            alignment_bytes as usize,
        ));
    if let Some(domain) = domain {
        builder = builder.label("domain", domain);
    }
    if let Some(object_set) = object_set {
        builder = builder.label("object_set", object_set);
    }
    if let Some(qos_tier) = qos_tier {
        builder = builder.label("qos_tier", qos_tier);
    }
    let storage = Arc::new(
        builder
            .build(test_future_expiry_ms())
            .expect("labeled storage client build should succeed"),
    );
    storage
        .register_local_memory()
        .expect("labeled storage memory should register");
    let runtime = storage.runtime_id().clone();
    metadata
        .upsert_client_lease(&storage.lease())
        .expect("labeled storage lease should upsert");
    test_storage_nodes().lock().push(storage);
    runtime
}

fn test_segment_target_chunks(
    transport: &TestTransport,
    segment_name: &str,
) -> Vec<SegmentTargetChunk> {
    let (base, len) = transport
        .segment_bounds(segment_name)
        .expect("test transport segment should expose target bounds");
    vec![SegmentTargetChunk {
        logical_offset: 0,
        target_offset: base,
        length_bytes: len,
    }]
}

fn test_storage_nodes() -> &'static Mutex<Vec<Arc<StoreClient>>> {
    static STORAGE_NODES: OnceLock<Mutex<Vec<Arc<StoreClient>>>> = OnceLock::new();
    STORAGE_NODES.get_or_init(|| Mutex::new(Vec::new()))
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

fn wait_for_active_stream_session(client: &ControlPlaneClient, context: &'static str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if client.active_stream_sessions() >= 1 {
            return;
        }
        sleep(Duration::from_millis(10));
    }
    panic!("{context}");
}

fn wait_for_storage_clock_hot(storage: &StoreClient, route: &ObjectRoute) {
    wait_for_storage_clock_route(storage, route, true);
}

fn wait_for_storage_clock_route(storage: &StoreClient, route: &ObjectRoute, require_hot: bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let found = {
            let clock = storage.storage_owner.clock.lock();
            clock
                .entries
                .iter()
                .filter_map(Option::as_ref)
                .any(|entry| {
                    entry.id.route_key == route.key
                        && route.replicas.iter().any(|replica| {
                            replica.owner == *storage.runtime_id()
                                && replica.segment_name == entry.id.segment_name
                                && replica.segment_offset == entry.id.segment_offset
                        })
                        && (!require_hot || entry.hot)
                })
        };
        if found {
            return;
        }
        sleep(Duration::from_millis(10));
    }
    if require_hot {
        panic!(
            "route {} did not become hot on storage owner {} in time",
            route.key.0,
            storage.runtime_id()
        );
    } else {
        panic!(
            "route {} was not tracked by storage owner {} in time",
            route.key.0,
            storage.runtime_id()
        );
    }
}

fn wait_for_authority_route(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> ObjectRoute {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        match crate::route_directory::authority_get(namespace, authority, key) {
            Ok(Some(route)) => return route,
            Ok(None) => {}
            Err(error) => panic!("authority route query failed: {error}"),
        }
        sleep(Duration::from_millis(10));
    }
    panic!(
        "route {} did not become visible on authority {}",
        key.0, authority
    );
}

fn tc_replica2_payload(key: &str) -> Vec<u8> {
    format!("tc-replica2-payload::{key}").into_bytes()
}

#[test]
fn hot_upgrade_handoff_is_published_after_draining() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata.clone(), "client-a")
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
fn register_local_memory_preserves_segment_name_and_publishes_transport_endpoint() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let logical_segment = SegmentName::new("logical-segment");
    let transport_endpoint = "189.144.184.2:12001";
    let transport_descriptor = r#"{"name":"189.144.184.2","protocol":"rdma","devices":[{"name":"mlx5_0","lid":0,"gid":"2001:db8::1"}],"buffers":[],"priority_matrix":{},"tcp_data_port":0}"#;
    let transport = Arc::new(TestTransport::new(transport_endpoint));
    transport.set_local_segment_descriptor(transport_descriptor);
    let client = StoreClientBuilder::new(metadata.clone(), "p2p-publish")
        .state(ClientLifecycleState::Active)
        .segment_name(logical_segment.0.clone())
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .register_local_memory()
        .expect("local memory should register");

    let segment = metadata
        .get_segment(client.runtime_id(), &logical_segment)
        .expect("segment lookup should succeed")
        .expect("logical segment should be published");
    assert_eq!(segment.segment_name, logical_segment);
    assert_eq!(
        segment.transport_endpoint.as_deref(),
        Some(transport_endpoint)
    );
    assert_eq!(
        segment.transport_segment_descriptor.as_deref(),
        Some(transport_descriptor)
    );
}

#[test]
fn routed_io_opens_transport_endpoint_while_routes_keep_segment_name() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let logical_segment = SegmentName::new("storage-logical-segment");
    let transport_endpoint = "189.144.184.2:12001";
    let transport_descriptor = r#"{"name":"189.144.184.2","protocol":"rdma","devices":[{"name":"mlx5_0","lid":0,"gid":"2001:db8::1"}],"buffers":[],"priority_matrix":{},"tcp_data_port":0}"#;
    let storage_transport = Arc::new(TestTransport::new(transport_endpoint));
    storage_transport.set_local_segment_descriptor(transport_descriptor);
    let writer_transport = Arc::new(storage_transport.peer("writer-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "p2p-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .segment_name(logical_segment.0.clone())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "p2p-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(writer_transport.clone())
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer scratch should register");

    writer
        .put("p2p-key", b"payload")
        .expect("writer should put through transport endpoint");
    assert_eq!(
        writer
            .get("p2p-key")
            .expect("writer should read through transport endpoint"),
        b"payload"
    );

    let route = writer
        .query_route("p2p-key")
        .expect("route query should succeed")
        .expect("route should exist");
    assert_eq!(route.replicas[0].segment_name, logical_segment);
    assert!(
        writer_transport
            .cached_segment_descriptors()
            .iter()
            .any(|(segment_name, descriptor)| {
                segment_name == transport_endpoint && descriptor == transport_descriptor
            }),
        "writer should preload the full TE descriptor before opening the P2P endpoint"
    );
}

#[test]
fn hot_upgrade_successor_discovery_uses_same_stable_higher_epoch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let predecessor = StoreClientBuilder::new(metadata.clone(), "upgrade-find")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7101")
        .segment_name("upgrade-find-old")
        .build(test_future_expiry_ms())
        .expect("predecessor build should succeed");
    let successor = StoreClientBuilder::new(metadata.clone(), "upgrade-find")
        .state(ClientLifecycleState::Standby)
        .rpc_address("127.0.0.1:7102")
        .segment_name("upgrade-find-new")
        .build(test_future_expiry_ms())
        .expect("successor build should succeed");
    let _other_stable = StoreClientBuilder::new(metadata, "upgrade-other")
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
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7111")
        .segment_name("upgrade-promote-old")
        .build(test_future_expiry_ms())
        .expect("predecessor build should succeed");
    let mut successor = StoreClientBuilder::new(metadata, "upgrade-promote")
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
    assert!(client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("tenant-a"), None, None),
            "same-key"
        )
        .expect("scope query should succeed")
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
fn remove_force_true_skips_missing_key() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("client-segment"));
    let client = StoreClientBuilder::new(metadata, "client-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .remove("missing-key", true)
        .expect("force remove should ignore missing key");

    let error = client
        .remove("missing-key", false)
        .expect_err("non-force remove should fail for missing key");
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[test]
fn batch_remove_force_true_skips_missing_keys() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("client-segment"));
    let client = StoreClientBuilder::new(metadata, "client-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .put("existing-key", b"data")
        .expect("put should succeed");
    let objects = [
        ObjectRef::new("existing-key"),
        ObjectRef::new("missing-key"),
    ];

    client
        .batch_remove(&objects, true)
        .expect("force batch remove should ignore missing keys");
    assert!(client
        .query_route("existing-key")
        .expect("route query should succeed")
        .is_none());
    assert!(client
        .query_route("missing-key")
        .expect("route query should succeed")
        .is_none());

    let error = client
        .batch_remove(&objects, false)
        .expect_err("non-force batch remove should fail for missing key");
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[test]
fn remove_retries_after_route_delete_conflict_with_fresher_current() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let block_key = ObjectKey::from_scope(
        &NamespaceScope::with_defaults(Some("default"), None, None),
        "conflict-key",
    );
    let metadata = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        block_key.0.as_str(),
    ));
    let writer_transport = Arc::new(TestTransport::new("delete-conflict-writer"));
    let remover_transport = Arc::new(TestTransport::new("delete-conflict-remover"));
    let writer = StoreClientBuilder::new(metadata.clone(), "delete-conflict-writer")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let remover = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "delete-conflict-remover")
            .state(ClientLifecycleState::Active)
            .route_control(RouteControlMode::MetadataOnly)
            .transport(remover_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("remover build should succeed"),
    );

    writer
        .register_local_memory()
        .expect("writer memory should register");
    remover
        .register_local_memory()
        .expect("remover memory should register");

    let route_v1 = writer
        .put("conflict-key", b"payload")
        .expect("seed put should succeed");

    metadata.arm_blocked_cas();
    let remover_task = {
        let remover = Arc::clone(&remover);
        std::thread::spawn(move || remover.remove("conflict-key", true))
    };
    assert!(
        metadata.wait_until_blocked(Duration::from_secs(1)),
        "delete CAS should block before the route update race is injected"
    );

    let mut route_v2 = route_v1.clone();
    let successor_runtime = ClientRuntimeId::new("delete-conflict-writer", ClientEpoch(2));
    metadata
        .put_handoff(&HandoffPlan {
            stable_id: route_v1.replicas[0].owner.stable_id.clone(),
            from: route_v1.replicas[0].owner.clone(),
            to: successor_runtime.clone(),
            kind: HandoffKind::HotUpgrade,
            barrier_version: route_v1.version.0,
            created_at_ms: now_ms(),
            deadline_ms: None,
        })
        .expect("handoff plan should publish");
    route_v2.version = route_v1.version.next();
    route_v2.replicas[0].owner = successor_runtime;
    route_v2.replicas[0].segment_name = SegmentName::new("delete-conflict-writer-successor");
    let cas = inner
        .compare_and_swap_object_route(&route_v1.key, Some(route_v1.version), Some(&route_v2))
        .expect("concurrent route update should succeed");
    assert!(
        cas.applied,
        "concurrent route update should become authoritative"
    );

    metadata.release_blocked_cas();
    remover_task
        .join()
        .expect("remove worker should join")
        .expect("remove should retry with the fresher current route");
    assert!(
        writer
            .query_route("conflict-key")
            .expect("route query should succeed")
            .is_none(),
        "route should be deleted after retrying with the fresher current route"
    );
}

#[test]
fn batch_remove_retries_after_route_delete_conflict_with_fresher_current() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let block_key = ObjectKey::from_scope(
        &NamespaceScope::with_defaults(Some("default"), None, None),
        "batch-conflict-key",
    );
    let metadata = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        block_key.0.as_str(),
    ));
    let writer_transport = Arc::new(TestTransport::new("batch-delete-conflict-writer"));
    let remover_transport = Arc::new(TestTransport::new("batch-delete-conflict-remover"));
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-delete-conflict-writer")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let remover = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "batch-delete-conflict-remover")
            .state(ClientLifecycleState::Active)
            .route_control(RouteControlMode::MetadataOnly)
            .transport(remover_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("remover build should succeed"),
    );

    writer
        .register_local_memory()
        .expect("writer memory should register");
    remover
        .register_local_memory()
        .expect("remover memory should register");

    let route_v1 = writer
        .put("batch-conflict-key", b"payload")
        .expect("seed put should succeed");

    metadata.arm_blocked_cas();
    let remover_task = {
        let remover = Arc::clone(&remover);
        std::thread::spawn(move || {
            remover.batch_remove(&[ObjectRef::new("batch-conflict-key")], true)
        })
    };
    assert!(
        metadata.wait_until_blocked(Duration::from_secs(1)),
        "batch delete CAS should block before the route update race is injected"
    );

    let mut route_v2 = route_v1.clone();
    let successor_runtime = ClientRuntimeId::new("batch-delete-conflict-writer", ClientEpoch(2));
    metadata
        .put_handoff(&HandoffPlan {
            stable_id: route_v1.replicas[0].owner.stable_id.clone(),
            from: route_v1.replicas[0].owner.clone(),
            to: successor_runtime.clone(),
            kind: HandoffKind::HotUpgrade,
            barrier_version: route_v1.version.0,
            created_at_ms: now_ms(),
            deadline_ms: None,
        })
        .expect("handoff plan should publish");
    route_v2.version = route_v1.version.next();
    route_v2.replicas[0].owner = successor_runtime;
    route_v2.replicas[0].segment_name = SegmentName::new("batch-delete-conflict-writer-successor");
    let cas = inner
        .compare_and_swap_object_route(&route_v1.key, Some(route_v1.version), Some(&route_v2))
        .expect("concurrent route update should succeed");
    assert!(
        cas.applied,
        "concurrent route update should become authoritative"
    );

    metadata.release_blocked_cas();
    remover_task
        .join()
        .expect("batch remove worker should join")
        .expect("batch remove should retry with the fresher current route");
    assert!(
        writer
            .query_route("batch-conflict-key")
            .expect("route query should succeed")
            .is_none(),
        "route should be deleted after batch remove retries with the fresher current route"
    );
}

#[test]
fn remove_retries_after_route_delete_conflict_with_standby_promotion_successor_current() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let block_key = ObjectKey::from_scope(
        &NamespaceScope::with_defaults(Some("default"), None, None),
        "standby-promotion-conflict-key",
    );
    let metadata = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        block_key.0.as_str(),
    ));
    let writer_transport = Arc::new(TestTransport::new("standby-promotion-writer"));
    let remover_transport = Arc::new(TestTransport::new("standby-promotion-remover"));
    let writer = StoreClientBuilder::new(metadata.clone(), "standby-promotion-writer")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let remover = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "standby-promotion-remover")
            .state(ClientLifecycleState::Active)
            .route_control(RouteControlMode::MetadataOnly)
            .transport(remover_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("remover build should succeed"),
    );

    writer
        .register_local_memory()
        .expect("writer memory should register");
    remover
        .register_local_memory()
        .expect("remover memory should register");

    let route_v1 = writer
        .put("standby-promotion-conflict-key", b"payload")
        .expect("seed put should succeed");

    metadata.arm_blocked_cas();
    let remover_task = {
        let remover = Arc::clone(&remover);
        std::thread::spawn(move || remover.remove("standby-promotion-conflict-key", true))
    };
    assert!(
        metadata.wait_until_blocked(Duration::from_secs(1)),
        "delete CAS should block before the standby-promotion route update is injected"
    );

    let mut route_v2 = route_v1.clone();
    let successor_runtime = ClientRuntimeId::new("standby-promotion-writer", ClientEpoch(2));
    metadata
        .put_handoff(&HandoffPlan {
            stable_id: route_v1.replicas[0].owner.stable_id.clone(),
            from: route_v1.replicas[0].owner.clone(),
            to: successor_runtime.clone(),
            kind: HandoffKind::HotStandbyPromotion,
            barrier_version: route_v1.version.0,
            created_at_ms: now_ms(),
            deadline_ms: None,
        })
        .expect("handoff plan should publish");
    route_v2.version = route_v1.version.next();
    route_v2.replicas[0].owner = successor_runtime;
    route_v2.replicas[0].segment_name = SegmentName::new("standby-promotion-writer-successor");
    let cas = inner
        .compare_and_swap_object_route(&route_v1.key, Some(route_v1.version), Some(&route_v2))
        .expect("concurrent route update should succeed");
    assert!(
        cas.applied,
        "concurrent route update should become authoritative"
    );

    metadata.release_blocked_cas();
    remover_task
        .join()
        .expect("remove worker should join")
        .expect("remove should retry with the standby-promotion successor route");
    assert!(
        writer
            .query_route("standby-promotion-conflict-key")
            .expect("route query should succeed")
            .is_none(),
        "route should be deleted after retrying with the standby-promotion successor route"
    );
}

#[test]
fn remove_force_true_keeps_conflict_for_fresher_rewrite_route() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let block_key = ObjectKey::from_scope(
        &NamespaceScope::with_defaults(Some("default"), None, None),
        "rewrite-conflict-key",
    );
    let metadata = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        block_key.0.as_str(),
    ));
    let writer_transport = Arc::new(TestTransport::new("rewrite-conflict-writer"));
    let remover_transport = Arc::new(TestTransport::new("rewrite-conflict-remover"));
    let writer = StoreClientBuilder::new(metadata.clone(), "rewrite-conflict-writer")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let remover = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "rewrite-conflict-remover")
            .state(ClientLifecycleState::Active)
            .route_control(RouteControlMode::MetadataOnly)
            .transport(remover_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("remover build should succeed"),
    );

    writer
        .register_local_memory()
        .expect("writer memory should register");
    remover
        .register_local_memory()
        .expect("remover memory should register");

    let route_v1 = writer
        .put("rewrite-conflict-key", b"payload")
        .expect("seed put should succeed");

    metadata.arm_blocked_cas();
    let remover_task = {
        let remover = Arc::clone(&remover);
        std::thread::spawn(move || remover.remove("rewrite-conflict-key", true))
    };
    assert!(
        metadata.wait_until_blocked(Duration::from_secs(1)),
        "delete CAS should block before the same-payload rewrite route is injected"
    );

    let mut route_v2 = route_v1.clone();
    route_v2.version = route_v1.version.next();
    route_v2.replicas[0].segment_name = SegmentName::new("rewrite-conflict-alt-segment");
    route_v2.replicas[0].offset = Some(route_v1.replicas[0].offset.unwrap_or_default() + 4096);
    route_v2.replicas[0].segment_offset = route_v1.replicas[0].segment_offset + 4096;
    let cas = inner
        .compare_and_swap_object_route(&route_v1.key, Some(route_v1.version), Some(&route_v2))
        .expect("concurrent same-payload rewrite route update should succeed");
    assert!(
        cas.applied,
        "concurrent same-payload rewrite route update should become authoritative"
    );

    metadata.release_blocked_cas();
    let error = remover_task
        .join()
        .expect("remove worker should join")
        .expect_err("force remove should keep conflict for a fresher rewrite route");
    assert!(
        matches!(error, StoreError::Conflict(_)),
        "expected conflict, got {error:?}"
    );
    assert_eq!(
        writer
            .query_route("rewrite-conflict-key")
            .expect("route query should succeed"),
        Some(route_v2),
        "same-payload rewrite route should stay authoritative after delete conflict"
    );
}

#[test]
fn remove_retries_after_route_delete_conflict_with_partial_multi_replica_handoff() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let block_key = ObjectKey::from_scope(
        &NamespaceScope::with_defaults(Some("default"), None, None),
        "multi-replica-conflict-key",
    );
    let metadata = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        block_key.0.as_str(),
    ));
    let writer_transport = Arc::new(TestTransport::new("multi-replica-conflict-writer"));
    let remover_transport = Arc::new(TestTransport::new("multi-replica-conflict-remover"));
    let writer = StoreClientBuilder::new(metadata.clone(), "multi-replica-conflict-writer")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let remover = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "multi-replica-conflict-remover")
            .state(ClientLifecycleState::Active)
            .route_control(RouteControlMode::MetadataOnly)
            .transport(remover_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("remover build should succeed"),
    );

    writer
        .register_local_memory()
        .expect("writer memory should register");
    remover
        .register_local_memory()
        .expect("remover memory should register");

    let object_id =
        mooncake_store_core::scoped_logical_object_id("default", "multi-replica-conflict-key");
    let checksum = payload_checksum(b"payload");
    let owner_a_v1 = ClientRuntimeId::new("multi-replica-store-a", ClientEpoch(1));
    let owner_a_v2 = ClientRuntimeId::new("multi-replica-store-a", ClientEpoch(2));
    let owner_b_v1 = ClientRuntimeId::new("multi-replica-store-b", ClientEpoch(1));
    let mut route_v1 = ObjectRoute {
        key: ObjectKey::from_logical_id(&object_id),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: Some(mooncake_store_core::DEFAULT_QOS_TIER.to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: writer.lease().compatibility.clone(),
        replicas: vec![
            ReplicaRoute {
                owner: owner_a_v1.clone(),
                segment_name: SegmentName::new("multi-replica-store-a-segment-v1"),
                offset: Some(1024),
                segment_offset: 0,
                length: 7,
                checksum: Some(checksum),
                tier: mooncake_store_core::ReplicaTier::Dram,
                priority: 0,
            },
            ReplicaRoute {
                owner: owner_b_v1.clone(),
                segment_name: SegmentName::new("multi-replica-store-b-segment-v1"),
                offset: Some(2048),
                segment_offset: 0,
                length: 7,
                checksum: Some(checksum),
                tier: mooncake_store_core::ReplicaTier::Dram,
                priority: 1,
            },
        ],
    };
    mooncake_store_core::apply_route_identity(&mut route_v1, &object_id);
    let published = inner
        .compare_and_swap_object_route(&route_v1.key, None, Some(&route_v1))
        .expect("seed route publish should succeed");
    assert!(published.applied, "seed route should publish");

    metadata
        .put_handoff(&HandoffPlan {
            stable_id: owner_a_v1.stable_id.clone(),
            from: owner_a_v1.clone(),
            to: owner_a_v2.clone(),
            kind: HandoffKind::HotUpgrade,
            barrier_version: route_v1.version.0,
            created_at_ms: now_ms(),
            deadline_ms: None,
        })
        .expect("handoff plan should publish");

    metadata.arm_blocked_cas();
    let remover_task = {
        let remover = Arc::clone(&remover);
        std::thread::spawn(move || remover.remove("multi-replica-conflict-key", true))
    };
    assert!(
        metadata.wait_until_blocked(Duration::from_secs(1)),
        "delete CAS should block before the partial multi-replica route update is injected"
    );

    let mut route_v2 = route_v1.clone();
    route_v2.version = route_v1.version.next();
    route_v2.replicas[0].owner = owner_a_v2;
    route_v2.replicas[0].segment_name = SegmentName::new("multi-replica-store-a-segment-v2");
    route_v2.replicas[0].offset = Some(4096);
    route_v2.replicas[0].segment_offset = 512;
    let cas = inner
        .compare_and_swap_object_route(&route_v1.key, Some(route_v1.version), Some(&route_v2))
        .expect("concurrent partial multi-replica route update should succeed");
    assert!(
        cas.applied,
        "concurrent partial multi-replica route update should become authoritative"
    );

    metadata.release_blocked_cas();
    remover_task
        .join()
        .expect("remove worker should join")
        .expect("remove should retry with the partial multi-replica successor route");
    assert!(
        writer
            .query_route("multi-replica-conflict-key")
            .expect("route query should succeed")
            .is_none(),
        "route should be deleted after retrying with the partial multi-replica successor route"
    );
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
    assert!(metrics.contains("mooncake_store_operation_total"));
    assert!(metrics.contains("operation=\"put\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get\",status=\"ok\""));
    assert!(metrics.contains("operation=\"put_local_copy\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get_local_copy\",status=\"ok\""));
    assert!(metrics.contains("mooncake_store_operation_bytes_out_total"));
}

#[test]
fn observability_metrics_render_remote_datapaths() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("metrics-remote-writer-segment"));
    let reader_transport = Arc::new(writer_transport.peer("metrics-remote-reader-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
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
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "client-metrics-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
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
        .contains("mooncake_store_replication_publish_duration_seconds_count{tenant=\"default\",result=\"ok\"}"));
    assert!(metrics
        .contains("mooncake_store_replication_publish_total{tenant=\"default\",result=\"ok\"}"));
    assert!(metrics.contains(
        "mooncake_store_transport_operation_total{tenant=\"default\",direction=\"write\",peer_kind=\"storage\",result=\"ok\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_transport_operation_total{tenant=\"default\",direction=\"read\",peer_kind=\"storage\",result=\"ok\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_transport_bytes_total{tenant=\"default\",direction=\"write\",peer_kind=\"storage\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_transport_bytes_total{tenant=\"default\",direction=\"read\",peer_kind=\"storage\"}"
    ));
    assert!(metrics
        .contains("mooncake_store_checksum_validation_total{tenant=\"default\",result=\"ok\"}"));
}

#[test]
fn observability_metrics_render_fast_batch_put_stages() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("metrics-fast-batch-segment"));
    publish_storage_node(
        &metadata,
        &transport,
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
        &metadata,
        &transport,
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
    assert_eq!(
        client
            .lease()
            .endpoints
            .labels
            .get("route")
            .map(String::as_str),
        Some("false")
    );
}

#[test]
fn builder_keeps_explicit_route_true_for_rw_only_client() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rw-explicit-route-segment"));
    let client = StoreClientBuilder::new(metadata, "rw-explicit-route")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(rw_only_config())
        .label("route", "true")
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
    assert_eq!(
        client
            .lease()
            .endpoints
            .labels
            .get("route")
            .map(String::as_str),
        Some("true")
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
fn lookup_runtime_lease_refreshes_when_snapshot_misses_runtime() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-refresh-segment"));
    let client_transport = Arc::new(storage_transport.peer("client-refresh-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-refresh")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    let client = StoreClientBuilder::new(metadata.clone(), "client-refresh")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(client_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .live_client_cache
        .lock()
        .store(vec![client.lease().clone()]);

    let runtime = storage.runtime_id().clone();
    let after_clobber = metadata.list_live_clients_calls();
    let lease = client
        .lookup_runtime_lease(&runtime)
        .expect("runtime lookup should refresh when the cached snapshot misses a live runtime");
    assert_eq!(lease.runtime, runtime);
    assert!(
        metadata.list_live_clients_calls() > after_clobber,
        "runtime lookup should force-refresh membership after a snapshot miss"
    );
}

#[test]
fn rw_only_client_can_expand_into_primary_segment() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rw-expand-segment"));
    let client = StoreClientBuilder::new(metadata, "rw-expand")
        .state(ClientLifecycleState::Active)
        .label("route", "true")
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
fn register_local_memory_keeps_initial_storage_single_despite_registration_limit() {
    with_test_numa_locations(&["cpu:0", "cpu:1"], || {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("split-storage-store"));
        transport.set_max_registration_bytes(Some(64));
        let client = StoreClientBuilder::new(metadata, "split-storage")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "true")
            .transport(transport.clone())
            .transport_factory(transport.factory())
            .local_memory(
                storage_config_with_bytes(160)
                    .scratch_bytes(16)
                    .location("cpu:0")
                    .numa_aware(true),
            )
            .build(test_future_expiry_ms())
            .expect("client build should succeed");

        client
            .register_local_memory()
            .expect("local memory registration should succeed");

        let capacities = client
            .list_segments()
            .expect("segments should list")
            .into_iter()
            .map(|segment| segment.capacity_bytes)
            .collect::<Vec<_>>();
        assert_eq!(capacities, vec![160]);
    });
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
    let route_calls_before = metadata.get_object_route_calls();
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
    let route_calls_after = metadata.get_object_route_calls();

    assert_eq!(after_second, after_build);
    assert_eq!(
        route_calls_after, route_calls_before,
        "embedded WRH get/query hot path must not query metadata routes when authorities resolve"
    );
}

#[test]
fn embedded_wrh_evacuation_route_facade_reuses_live_snapshot() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let store_a_transport = Arc::new(TestTransport::new("evacuation-authority-cache-a-segment"));
    let store_b_transport =
        Arc::new(store_a_transport.peer("evacuation-authority-cache-b-segment"));
    let router_transport =
        Arc::new(store_a_transport.peer("evacuation-authority-cache-router-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "evacuation-authority-cache-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "evacuation-authority-cache-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "evacuation-authority-cache-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");

    wait_for_membership_convergence(&[&store_a, &store_b, &router]);

    let key = "evacuation-authority-cache-key";
    let value = b"payload".to_vec();
    router
        .put_with_policy(
            key,
            &value,
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owner(store_a.runtime_id().storage_key()),
        )
        .expect("seed put should succeed");
    let route = router
        .query_route(key)
        .expect("route query should succeed")
        .expect("route should exist");
    assert_eq!(route.replicas[0].owner, *store_a.runtime_id());

    let after_seed = metadata.list_live_clients_calls();
    let policy = router
        .migration_policy_for_route(&route, router.runtime_id())
        .expect("migration policy should resolve from the shared snapshot");
    let after_policy = metadata.list_live_clients_calls();
    assert!(
        !policy.preferred_storage_owners.is_empty(),
        "migration policy should retain at least one active target"
    );
    assert_eq!(
        after_policy, after_seed,
        "migration policy should reuse the shared live snapshot"
    );

    router
        .ensure_object_route_at_least(&route)
        .expect("route facade update should succeed");
    let after_sync = metadata.list_live_clients_calls();
    assert_eq!(
        after_sync, after_seed,
        "route facade update should not refresh live-authority snapshots"
    );
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
fn stable_debug_log_sample_is_deterministic_and_sparse() {
    let key = ["route_authority_object_route", "namespace", "key-42"];
    assert_eq!(stable_debug_log_sample(&key), stable_debug_log_sample(&key));

    let sampled = (0..1024)
        .filter(|index| {
            let key = format!("key-{index}");
            stable_debug_log_sample(&["storage_owner_evicted_replica", &key])
        })
        .count();
    assert!((1..32).contains(&sampled), "sampled={sampled}");
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
        &metadata.inner,
        &writer_transport,
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
    let route = router
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
    wait_for_active_stream_session(
        &reader.control_client,
        "async route hit reporting should open a reusable control stream",
    );
    wait_for_storage_clock_hot(&storage, &route);
}

#[test]
fn routed_batch_put_publishes_replicated_route_with_absolute_offsets() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("router-segment"));
    let owner_a = publish_storage_node(&metadata, &transport, "storage-a", "seg-a", "pool-a");
    let owner_b = publish_storage_node(&metadata, &transport, "storage-b", "seg-b", "pool-a");
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
    wait_for_runtime_visibility(&client, &owner_a);
    wait_for_runtime_visibility(&client, &owner_b);

    let payload = b"routed-payload";
    let routes = client
        .batch_put(&[PutRequest::new("key-a", payload).tenant("tenant-a")])
        .expect("batch routed put should succeed");

    assert_eq!(routes.len(), 1);
    let route = &routes[0];
    assert_eq!(route.key, ObjectKey::new("tenant-a::key-a"));
    assert_eq!(
        route.namespace,
        Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None))
    );
    assert_eq!(route.logical_key.as_deref(), Some("key-a"));
    assert_eq!(
        route.canonical_key.as_deref(),
        Some("tenant-a/default/default/key-a")
    );
    assert_eq!(route.sharing_scope.as_deref(), Some("tenant-a"));
    assert_eq!(route.qos_tier.as_deref(), Some("default"));
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
        let offset = replica
            .offset
            .expect("new writes should publish target offset");
        assert!(offset >= base);
        assert!(offset + replica.length <= base + len);
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
    let reader_transport = Arc::new(writer_transport.peer("reader-segment"));
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
fn routed_read_probes_cached_remote_segment_before_reuse() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cached-probe-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("cached-probe-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("cached-probe-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("cached-probe-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "cached-probe-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cached-probe-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "cached-probe-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cached-probe-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
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
            "cached-probe-key",
            b"cached-probe-payload",
            &ReplicationPolicy::new()
                .replica_count(2)
                .prefer_local(false)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("replicated put should succeed");

    assert_eq!(
        reader
            .get("cached-probe-key")
            .expect("first read should cache the primary remote segment"),
        b"cached-probe-payload"
    );

    let primary_segment = store_a
        .segment_name()
        .expect("store-a segment should exist");
    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove(&primary_segment.0)
            .expect("primary segment handle should exist");
        state
            .segments_by_handle
            .remove(&handle)
            .expect("primary segment body should exist");
    }

    assert_eq!(
        reader
            .get("cached-probe-key")
            .expect("second read should probe cached primary and fail over"),
        b"cached-probe-payload"
    );
    assert!(
        reader
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "dead primary must be quarantined even when its segment handle is cached"
    );
    let repaired = reader
        .query_route("cached-probe-key")
        .expect("repaired route query should succeed")
        .expect("repaired route should exist");
    assert_eq!(repaired.replicas.len(), 1);
    assert_eq!(repaired.replicas[0].owner, *store_b.runtime_id());
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
        (
            "batch-transport-failed-key-a",
            b"batch-transport-payload-a".as_slice(),
        ),
        (
            "batch-transport-failed-key-b",
            b"batch-transport-payload-b".as_slice(),
        ),
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
fn registered_batch_get_fails_over_after_primary_transport_failure() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new(
        "registered-batch-transport-failed-a-segment",
    ));
    let store_b_transport =
        Arc::new(store_a_transport.peer("registered-batch-transport-failed-b-segment"));
    let writer_transport =
        Arc::new(store_a_transport.peer("registered-batch-transport-failed-writer-segment"));
    let reader_transport =
        Arc::new(store_a_transport.peer("registered-batch-transport-failed-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "registered-batch-failed-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "registered-batch-failed-pool")
        .label("storage", "true")
        .label("route_scope", "registered-batch-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "registered-batch-failed-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "registered-batch-failed-pool")
        .label("storage", "true")
        .label("route_scope", "registered-batch-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "registered-batch-failed-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "registered-batch-failed-pool")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "registered-batch-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "registered-batch-failed-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "registered-batch-failed-pool")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "registered-batch-failed-scope")
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
        ("registered-batch-failed-key-a", b"registered-a".as_slice()),
        ("registered-batch-failed-key-b", b"registered-b".as_slice()),
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
            .remove("registered-batch-transport-failed-a-segment")
            .expect("primary segment handle should exist");
        state.segments_by_handle.remove(&handle);
    }

    let mut target = [0u8; 32];
    reader
        .register_buffer(target.as_mut_ptr().cast(), target.len())
        .expect("reader register buffer should succeed");
    let (first, tail) = target.split_at_mut(12);
    let (_, second) = tail.split_at_mut(4);
    let sizes = reader
        .batch_get_into(&mut [
            GetRequest::new("registered-batch-failed-key-a", first),
            GetRequest::new("registered-batch-failed-key-b", &mut second[..12]),
        ])
        .expect("registered batch_get_into should fail over to surviving replicas");

    assert_eq!(sizes, vec![12, 12]);
    assert_eq!(&target[..12], b"registered-a");
    assert_eq!(&target[16..28], b"registered-b");
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
fn tc05_style_replica2_kill_hash_prefix_changes_victim_coverage() {
    const NUM_KEYS: usize = 30;

    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("tc05-hash-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("tc05-hash-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("tc05-hash-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("tc05-hash-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("tc05-hash-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "tc05-hash-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "tc05-hash-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "tc05-hash-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "tc05-hash-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "tc05-hash-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "tc05-hash-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "tc05-hash-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "tc05-hash-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "tc05-hash-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "tc05-hash-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    let victim = store_a.runtime_id().clone();
    let ownership_pattern = |prefix: &str| -> Vec<(String, bool)> {
        let mut result = Vec::with_capacity(NUM_KEYS);
        for index in 0..NUM_KEYS {
            let key = format!("{prefix}-{index}");
            let payload = tc_replica2_payload(&key);
            let route = writer
                .put_with_policy(
                    &key,
                    &payload,
                    &ReplicationPolicy::new()
                        .replica_count(2)
                        .prefer_local(false),
                )
                .expect("replica=2 put should succeed");
            let victim_owned = route.replicas.iter().any(|replica| replica.owner == victim);
            result.push((key, victim_owned));
        }
        result
    };

    let tcp_pattern = ownership_pattern("tc-replica2-tcp");
    let rdma_pattern = ownership_pattern("tc-replica2-rdma");
    let tcp_victim_owned = tcp_pattern
        .iter()
        .filter(|(_, victim_owned)| *victim_owned)
        .count();
    let rdma_victim_owned = rdma_pattern
        .iter()
        .filter(|(_, victim_owned)| *victim_owned)
        .count();

    assert!(
        tcp_victim_owned > 0 && tcp_victim_owned < NUM_KEYS,
        "TC-05 style free placement should leave some keys on victim and some keys away from victim",
    );
    assert!(
        rdma_victim_owned > 0 && rdma_victim_owned < NUM_KEYS,
        "TC-10 style key prefix should also only cover victim on a subset of keys",
    );
    assert_ne!(
        tcp_pattern
            .iter()
            .map(|(_, victim_owned)| *victim_owned)
            .collect::<Vec<_>>(),
        rdma_pattern
            .iter()
            .map(|(_, victim_owned)| *victim_owned)
            .collect::<Vec<_>>(),
        "changing only the key prefix should perturb which keys place a replica on the killed node",
    );

    let tcp_key_without_victim = tcp_pattern
        .iter()
        .find_map(|(key, victim_owned)| (!*victim_owned).then_some(key.clone()))
        .expect("tc-replica2-tcp should include a key that never touched victim");
    let tcp_key_with_victim = tcp_pattern
        .iter()
        .find_map(|(key, victim_owned)| (*victim_owned).then_some(key.clone()))
        .expect("tc-replica2-tcp should include a key that used victim");

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("tc05-hash-a-segment")
            .expect("victim segment handle should exist");
        state
            .segments_by_handle
            .remove(&handle)
            .expect("victim segment body should exist");
    }

    assert_eq!(
        reader
            .get(&tcp_key_without_victim)
            .expect("key that never used victim should still read"),
        tc_replica2_payload(&tcp_key_without_victim),
    );
    assert_eq!(
        reader
            .get(&tcp_key_with_victim)
            .expect("key that lost victim should fail over to its surviving replica"),
        tc_replica2_payload(&tcp_key_with_victim),
    );
}

#[test]
fn tc05_style_replica2_kill_survives_when_every_key_loses_victim_replica() {
    const NUM_KEYS: usize = 30;

    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("tc05-forced-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("tc05-forced-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("tc05-forced-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("tc05-forced-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("tc05-forced-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "tc05-forced-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "tc05-forced-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "tc05-forced-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "tc05-forced-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "tc05-forced-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "tc05-forced-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "tc05-forced-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "tc05-forced-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "tc05-forced-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "tc05-forced-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    let victim = store_a.runtime_id().clone();
    let survivor = store_b.runtime_id().clone();

    for index in 0..NUM_KEYS {
        let key = format!("tc-replica2-forced-{index}");
        let payload = tc_replica2_payload(&key);
        let route = writer
            .put_with_policy(
                &key,
                &payload,
                &ReplicationPolicy::new()
                    .replica_count(2)
                    .prefer_local(false)
                    .preferred_storage_owners([victim.storage_key(), survivor.storage_key()]),
            )
            .expect("forced victim+survivor put should succeed");
        let owners = route
            .replicas
            .iter()
            .map(|replica| replica.owner.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            owners,
            BTreeSet::from([victim.clone(), survivor.clone()]),
            "each TC-05 key must actually lose one replica on the victim after kill",
        );
    }

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("tc05-forced-a-segment")
            .expect("victim segment handle should exist");
        state
            .segments_by_handle
            .remove(&handle)
            .expect("victim segment body should exist");
    }

    for index in 0..NUM_KEYS {
        let key = format!("tc-replica2-forced-{index}");
        assert_eq!(
            reader
                .get(&key)
                .expect("reader should fail over to the surviving forced replica"),
            tc_replica2_payload(&key),
        );
    }
}

#[test]
fn suspect_authority_quarantine_is_shared_across_clients() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let suspect_runtime_cache = shared_suspect_runtime_cache(&metadata.route_namespace());
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

    let dead_runtime = store_a.runtime_id().clone();
    let first = reader_a.get("shared-suspect-key");
    assert!(
        matches!(
            first,
            Ok(ref value) if value == b"shared-suspect-payload"
        ) || matches!(
            first,
            Err(StoreError::NotFound(_))
                | Err(StoreError::Transport(_))
                | Err(StoreError::InvalidState(_))
        ),
        "first reader should either fail over immediately or surface the dead replica"
    );
    assert!(
        suspect_runtime_cache.lock().contains(&dead_runtime),
        "first reader should mark the dead replica runtime as suspect in the shared cache"
    );
    assert!(
        reader_a
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "first reader should quarantine the failed storage runtime"
    );
    assert!(
        reader_b
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "second reader should observe the shared quarantine entry"
    );
    assert_eq!(
        reader_b
            .get("shared-suspect-key")
            .expect(
                "second reader should reuse the shared quarantine and avoid a fresh dead-replica failure"
            ),
        b"shared-suspect-payload"
    );
}

#[test]
fn request_deadline_failure_does_not_quarantine_remote_runtime() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("deadline-store-segment"));
    let reader_transport = Arc::new(store_transport.peer("deadline-reader-segment"));

    let store = StoreClientBuilder::new(metadata.clone(), "deadline-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "deadline-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let reader = StoreClientBuilder::new(metadata, "deadline-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "deadline-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    let replica = ReplicaRoute {
        owner: store.runtime_id().clone(),
        segment_name: SegmentName::new("deadline-store-segment"),
        offset: Some(0),
        segment_offset: 0,
        length: 4,
        checksum: None,
        tier: mooncake_store_core::ReplicaTier::Dram,
        priority: 0,
    };
    let resolved = ResolvedObject {
        tenant: "default".to_string(),
        key: "deadline-key".to_string(),
        route: mooncake_store_core::ObjectRoute {
            key: reader.scoped_key("default", "deadline-key"),
            version: RouteVersion(1),
            state: mooncake_store_core::RouteState::Active,
            namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
            logical_key: Some("deadline-key".to_string()),
            canonical_key: None,
            sharing_scope: Some("default".to_string()),
            qos_tier: Some("default".to_string()),
            compatibility: reader.lease.compatibility.clone(),
            replicas: vec![replica.clone()],
        },
        replica,
        fallback_replicas: VecDeque::new(),
    };

    reader.note_remote_read_failure(
        &[&resolved],
        &StoreError::Transport("request deadline exceeded".to_string()),
        "deadline_test",
        false,
    );

    assert!(
        !reader
            .suspect_runtime_cache
            .lock()
            .contains(store.runtime_id()),
        "request deadline exhaustion must not quarantine a runtime without a stall signal"
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
fn routed_put_retries_after_mid_transfer_failure_and_uses_surviving_peer() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("put-retry-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("put-retry-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("put-retry-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("put-retry-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "put-retry-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "put-retry-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "put-retry-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "put-retry-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
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

    let store_a_segment = store_a
        .segment_name()
        .expect("store-a segment should exist");
    store_a_transport.fail_next_submit_for_segment(&store_a_segment.0);

    let route = writer
        .put_with_policy(
            "put-retry-key",
            b"put-retry-payload",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .with_soft_pin(true)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("put should retry after the first target fails mid-transfer");

    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, *store_b.runtime_id());
    assert!(
        writer
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "failed write target should be quarantined before retrying placement"
    );
    assert_eq!(
        reader
            .get("put-retry-key")
            .expect("reader should see retried write"),
        b"put-retry-payload"
    );
}

#[test]
fn routed_put_default_policy_retries_after_mid_transfer_failure() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("put-default-retry-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("put-default-retry-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("put-default-retry-writer-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "put-default-retry-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "put-default-retry-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata, "put-default-retry-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
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

    let store_a_segment = store_a
        .segment_name()
        .expect("store-a segment should exist");
    store_a_transport.fail_next_submit_for_segment(&store_a_segment.0);

    let route = writer
        .put_with_policy(
            "put-default-retry-key",
            b"put-default-retry-payload",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("default put should retry after the first target fails mid-transfer");

    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, *store_b.runtime_id());
    assert!(
        writer
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "failed write target should be quarantined before retrying placement"
    );
}

#[test]
fn routed_put_retries_past_default_limit_for_soft_preferred_owners() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root_transport = Arc::new(TestTransport::new("put-many-retry-store-0-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut stores = Vec::new();
    let mut store_transports = Vec::new();
    for index in 0..6 {
        let transport = if index == 0 {
            root_transport.clone()
        } else {
            Arc::new(root_transport.peer(&format!("put-many-retry-store-{index}-segment")))
        };
        let store =
            StoreClientBuilder::new(metadata.clone(), format!("put-many-retry-store-{index}"))
                .state(ClientLifecycleState::Active)
                .label("pool", "pool-a")
                .label("storage", "true")
                .route_control(RouteControlMode::MetadataOnly)
                .live_client_sync_interval(fast_live_client_sync_interval())
                .transport(transport.clone())
                .local_memory(storage_config())
                .build(test_future_expiry_ms())
                .expect("store build should succeed");
        store_transports.push(transport);
        stores.push(store);
    }

    let writer_transport = Arc::new(root_transport.peer("put-many-retry-writer-segment"));
    let writer = StoreClientBuilder::new(metadata, "put-many-retry-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    for store in &stores {
        store
            .register_local_memory()
            .expect("store memory should register");
    }
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let mut clients = stores.iter().collect::<Vec<_>>();
    clients.push(&writer);
    wait_for_membership_convergence(&clients);

    for (store, transport) in stores.iter().zip(store_transports.iter()).take(5) {
        let segment = store.segment_name().expect("store segment should exist");
        transport.fail_next_submit_for_segment(&segment.0);
    }
    let preferred_owners = stores
        .iter()
        .map(|store| store.runtime_id().storage_key())
        .collect::<Vec<_>>();

    let route = writer
        .put_with_policy(
            "put-many-retry-key",
            b"put-many-retry-payload",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners(preferred_owners),
        )
        .expect("put should keep retrying soft owners until a live target succeeds");

    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, *stores[5].runtime_id());
    for store in stores.iter().take(5) {
        assert!(
            writer
                .suspect_runtime_cache
                .lock()
                .contains(store.runtime_id()),
            "failed write target should be quarantined before trying later soft owners"
        );
    }
}

#[test]
fn routed_batch_put_from_retries_after_mid_transfer_failure_and_uses_surviving_peer() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("batch-put-retry-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("batch-put-retry-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("batch-put-retry-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("batch-put-retry-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "batch-put-retry-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "batch-put-retry-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-put-retry-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "batch-put-retry-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
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

    let source_a = b"batch-put-retry-payload-a".to_vec();
    let source_b = b"batch-put-retry-payload-b".to_vec();
    writer
        .register_buffer(source_a.as_ptr() as *mut c_void, source_a.len())
        .expect("source-a buffer should register");
    writer
        .register_buffer(source_b.as_ptr() as *mut c_void, source_b.len())
        .expect("source-b buffer should register");

    let store_a_segment = store_a
        .segment_name()
        .expect("store-a segment should exist");
    store_a_transport.fail_next_submit_for_segment(&store_a_segment.0);

    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false)
        .preferred_storage_owners([
            store_a.runtime_id().storage_key(),
            store_b.runtime_id().storage_key(),
        ]);
    let routes = writer
        .batch_put_from(&[
            PutFromRequest::new(
                "batch-put-retry-key-a",
                source_a.as_ptr().cast(),
                source_a.len(),
            )
            .replication(policy.clone()),
            PutFromRequest::new(
                "batch-put-retry-key-b",
                source_b.as_ptr().cast(),
                source_b.len(),
            )
            .replication(policy),
        ])
        .expect("batch_put_from should retry after the first target fails mid-transfer");

    assert_eq!(routes.len(), 2);
    for route in &routes {
        assert_eq!(route.replicas.len(), 1);
        assert_eq!(route.replicas[0].owner, *store_b.runtime_id());
    }
    assert!(
        writer
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "failed batch write target should be quarantined before retrying placement"
    );
    assert_eq!(
        reader
            .get("batch-put-retry-key-a")
            .expect("reader should see first retried write"),
        source_a
    );
    assert_eq!(
        reader
            .get("batch-put-retry-key-b")
            .expect("reader should see second retried write"),
        source_b
    );
}

#[test]
fn routed_batch_put_from_replans_after_membership_refresh_and_uses_new_runtime() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("batch-put-replan-a-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("batch-put-replan-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("batch-put-replan-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "batch-put-replan-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-put-replan-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "batch-put-replan-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(reader_transport)
        .local_memory(rw_only_config())
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
    wait_for_membership_convergence(&[&store_a, &writer, &reader]);

    let source = b"batch-put-replan-payload".to_vec();
    writer
        .register_buffer(source.as_ptr() as *mut c_void, source.len())
        .expect("source buffer should register");

    // Simulate a rollout successor appearing after the writer already cached membership.
    let store_b_transport = Arc::new(store_a_transport.peer("batch-put-replan-b-segment"));
    let store_b = StoreClientBuilder::new(metadata.clone(), "batch-put-replan-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");

    // Simulate a stale shared membership view that predates the successor rollout.
    writer
        .live_client_cache
        .lock()
        .store(vec![store_a.lease(), writer.lease(), reader.lease()]);

    let store_a_segment = store_a
        .segment_name()
        .expect("store-a segment should exist");
    store_a_transport.fail_next_submit_for_segment(&store_a_segment.0);
    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false)
        .preferred_storage_owners([store_a.runtime_id().storage_key()]);

    let routes = writer
        .batch_put_from(&[PutFromRequest::new(
            "batch-put-replan-key",
            source.as_ptr().cast(),
            source.len(),
        )
        .replication(policy)])
        .expect("batch_put_from should replan after membership refresh");

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].replicas.len(), 1);
    assert_eq!(routes[0].replicas[0].owner, *store_b.runtime_id());
    assert!(
        writer
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "failed stale target should be quarantined before replanning"
    );
    assert_eq!(
        reader
            .get("batch-put-replan-key")
            .expect("reader should see replanned write"),
        source
    );
}

#[test]
fn routed_batch_put_from_mixed_batch_keeps_live_targets_when_one_ranked_runtime_is_missing() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let metadata = Arc::new(RecoverableMetadataBackend::new(inner));
    let stale_transport = Arc::new(TestTransport::new("batch-put-mixed-missing-a-segment"));
    let live_transport = Arc::new(stale_transport.peer("batch-put-mixed-missing-b-segment"));
    let writer_transport = Arc::new(stale_transport.peer("batch-put-mixed-missing-writer-segment"));
    let reader_transport = Arc::new(stale_transport.peer("batch-put-mixed-missing-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let stale_store = StoreClientBuilder::new(metadata.clone(), "batch-put-mixed-missing-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(stale_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("stale store build should succeed");
    let live_store = StoreClientBuilder::new(metadata.clone(), "batch-put-mixed-missing-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(live_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("live store build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-put-mixed-missing-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner.clone(), 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "batch-put-mixed-missing-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    stale_store
        .register_local_memory()
        .expect("stale store memory should register");
    live_store
        .register_local_memory()
        .expect("live store memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&stale_store, &live_store, &writer, &reader]);

    let cached_membership = vec![
        stale_store.lease(),
        live_store.lease(),
        writer.lease(),
        reader.lease(),
    ];
    metadata.hide_runtime_metadata(stale_store.runtime_id());
    writer.live_client_cache.lock().store(cached_membership);

    let mut stale_first_key = None;
    let mut live_first_key = None;
    for index in 0..512usize {
        let key = format!("batch-put-mixed-missing-key-{index}");
        let ranked = planner
            .ranked_candidates(&writer, &ObjectRef::new(&key))
            .expect("ranked candidates should resolve from the cached membership");
        match ranked.first() {
            Some(runtime) if *runtime == *stale_store.runtime_id() && stale_first_key.is_none() => {
                stale_first_key = Some(key.clone());
            }
            Some(runtime) if *runtime == *live_store.runtime_id() && live_first_key.is_none() => {
                live_first_key = Some(key.clone());
            }
            _ => {}
        }
        if stale_first_key.is_some() && live_first_key.is_some() {
            break;
        }
    }
    let stale_first_key = stale_first_key.expect("one key should rank the hidden runtime first");
    let live_first_key = live_first_key.expect("one key should rank the surviving runtime first");

    let source_a = b"batch-put-mixed-missing-payload-a".to_vec();
    let source_b = b"batch-put-mixed-missing-payload-b".to_vec();
    writer
        .register_buffer(source_a.as_ptr() as *mut c_void, source_a.len())
        .expect("source-a buffer should register");
    writer
        .register_buffer(source_b.as_ptr() as *mut c_void, source_b.len())
        .expect("source-b buffer should register");

    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false);
    let routes = writer
        .batch_put_from(&[
            PutFromRequest::new(
                stale_first_key.as_str(),
                source_a.as_ptr().cast(),
                source_a.len(),
            )
            .replication(policy.clone()),
            PutFromRequest::new(
                live_first_key.as_str(),
                source_b.as_ptr().cast(),
                source_b.len(),
            )
            .replication(policy),
        ])
        .expect(
            "batch_put_from should preserve live targets even when one ranked runtime vanished",
        );

    assert_eq!(routes.len(), 2);
    for route in &routes {
        assert_eq!(route.replicas.len(), 1);
        assert_eq!(route.replicas[0].owner, *live_store.runtime_id());
    }
    assert_eq!(
        reader
            .get(stale_first_key.as_str())
            .expect("reader should fetch the first batch payload"),
        source_a
    );
    assert_eq!(
        reader
            .get(live_first_key.as_str())
            .expect("reader should fetch the second batch payload"),
        source_b
    );
}

#[test]
fn routed_batch_put_from_retries_after_reserve_phase_membership_refresh_and_uses_new_runtime() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("batch-put-reserve-retry-a-segment"));
    let writer_transport =
        Arc::new(store_a_transport.peer("batch-put-reserve-retry-writer-segment"));
    let reader_transport =
        Arc::new(store_a_transport.peer("batch-put-reserve-retry-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut store_a = StoreClientBuilder::new(metadata.clone(), "batch-put-reserve-retry-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-put-reserve-retry-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "batch-put-reserve-retry-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(reader_transport)
        .local_memory(rw_only_config())
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
    wait_for_membership_convergence(&[&store_a, &writer, &reader]);

    let source = b"batch-put-reserve-retry-payload".to_vec();
    writer
        .register_buffer(source.as_ptr() as *mut c_void, source.len())
        .expect("source buffer should register");

    // Simulate a rollout successor appearing after the writer already cached membership.
    let store_b_transport = Arc::new(store_a_transport.peer("batch-put-reserve-retry-b-segment"));
    let store_b = StoreClientBuilder::new(metadata.clone(), "batch-put-reserve-retry-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");

    // Simulate a stale shared membership view that predates the successor rollout.
    writer
        .live_client_cache
        .lock()
        .store(vec![store_a.lease(), writer.lease(), reader.lease()]);

    // The predecessor stops accepting new reservations before the writer refreshes membership.
    store_a
        .enter_draining()
        .expect("store-a should enter draining before reserve retry");

    let routes = writer
        .batch_put_from(&[PutFromRequest::new(
            "batch-put-reserve-retry-key",
            source.as_ptr().cast(),
            source.len(),
        )])
        .expect("batch_put_from should retry after reserve-stage membership refresh");

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].replicas.len(), 1);
    assert_eq!(routes[0].replicas[0].owner, *store_b.runtime_id());
    assert_eq!(
        reader
            .get("batch-put-reserve-retry-key")
            .expect("reader should see reserve-stage retried write"),
        source
    );
}

#[test]
fn routed_batch_put_from_waits_through_handoff_gap_until_successor_becomes_active() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("batch-put-handoff-gap-a-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("batch-put-handoff-gap-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("batch-put-handoff-gap-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut store_a = StoreClientBuilder::new(metadata.clone(), "batch-put-handoff-gap-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let mut store_b = StoreClientBuilder::new(metadata.clone(), "batch-put-handoff-gap-store-b")
        .state(ClientLifecycleState::Standby)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(Arc::new(
            store_a_transport.peer("batch-put-handoff-gap-b-segment"),
        ))
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-put-handoff-gap-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "batch-put-handoff-gap-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(reader_transport)
        .local_memory(rw_only_config())
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

    let source = b"batch-put-handoff-gap-payload".to_vec();
    writer
        .register_buffer(source.as_ptr() as *mut c_void, source.len())
        .expect("source buffer should register");

    // The writer starts from a stale view that still ranks the predecessor.
    writer
        .live_client_cache
        .lock()
        .store(vec![store_a.lease(), writer.lease(), reader.lease()]);
    store_a
        .enter_draining()
        .expect("store-a should stop accepting new writes");

    std::thread::scope(|scope| {
        let writer_result = scope.spawn(|| {
            writer.batch_put_from(&[PutFromRequest::new(
                "batch-put-handoff-gap-key",
                source.as_ptr().cast(),
                source.len(),
            )])
        });

        sleep(Duration::from_millis(150));
        store_b
            .activate()
            .expect("store-b should become active after the handoff gap");

        let routes = writer_result
            .join()
            .expect("writer thread should join")
            .expect("batch_put_from should survive the handoff gap");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].replicas.len(), 1);
        assert_eq!(routes[0].replicas[0].owner, *store_b.runtime_id());
    });

    assert_eq!(
        reader
            .get("batch-put-handoff-gap-key")
            .expect("reader should see the write after successor activation"),
        source
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
fn embedded_wrh_query_route_ignores_metadata_after_authority_miss() {
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

    assert!(
        reader
            .query_route("route-repair-metadata")
            .expect("metadata route lookup should still succeed")
            .is_none(),
        "embedded WRH must ignore metadata-only routes after authority miss"
    );
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
    assert!(
        matches!(
            reader.get("route-repair-metadata"),
            Err(StoreError::NotFound(_))
        ),
        "embedded WRH get should fail closed when only metadata still has the route"
    );
}

#[test]
fn embedded_wrh_route_publish_mirrors_secondaries_asynchronously() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("async-mirror-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("async-mirror-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("async-mirror-writer-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "async-mirror-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "async-mirror-scope")
        .route_topk(2)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "async-mirror-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "async-mirror-scope")
        .route_topk(2)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "async-mirror-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "async-mirror-scope")
        .route_topk(2)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
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

    let route = writer
        .put_with_policy(
            "async-mirror-key",
            b"async-mirror-payload",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([store_a.runtime_id().storage_key()]),
        )
        .expect("routed put should publish a primary route");
    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new("default::async-mirror-key");

    let store_a_route =
        wait_for_authority_route(&namespace, &store_a.runtime_id().stable_id, &scoped_key);
    let store_b_route =
        wait_for_authority_route(&namespace, &store_b.runtime_id().stable_id, &scoped_key);
    assert_eq!(store_a_route, route);
    assert_eq!(store_b_route, route);
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
        .label("route_weight", "1000000")
        .route_topk(3)
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
        .route_topk(3)
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
        .route_topk(3)
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
        .route_topk(3)
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
        .route_topk(3)
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
    divergent_route.replicas[0].offset = divergent_route.replicas[0]
        .offset
        .map(|offset| offset.saturating_add(1));
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
    assert_eq!(repaired_route.version, fresh_route.version);
    assert_eq!(repaired_route.replicas, fresh_route.replicas);
    let store_b_route = crate::route_directory::authority_get(
        &namespace,
        &store_b.runtime_id().stable_id,
        &scoped_key,
    )
    .expect("store-b repaired route should be readable")
    .expect("store-b repaired route should exist");
    let store_c_route = crate::route_directory::authority_get(
        &namespace,
        &store_c.runtime_id().stable_id,
        &scoped_key,
    )
    .expect("store-c repaired route should be readable")
    .expect("store-c repaired route should exist");
    assert_eq!(store_b_route.version, fresh_route.version);
    assert_eq!(store_b_route.replicas, fresh_route.replicas);
    assert_eq!(store_c_route.version, fresh_route.version);
    assert_eq!(store_c_route.replicas, fresh_route.replicas);
    assert_eq!(
        reader
            .get("route-repair-stale-old")
            .expect("reader get should succeed after divergent-authority repair"),
        b"route-repair-stale-old-v2"
    );
}

#[test]
fn embedded_wrh_route_directory_ignores_pool_boundaries_by_default() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("cross-pool-writer-segment"));
    let reader_transport = Arc::new(writer_transport.peer("cross-pool-reader-segment"));
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-cross-pool")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-reclaim")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "reader-cross-pool")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
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
fn routed_batch_put_from_treats_route_conflicts_as_per_key_success() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner,
        "default::batch-put-from-conflict-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let store_transport = Arc::new(TestTransport::new("batch-put-conflict-store-segment"));
    let writer_a_transport = Arc::new(store_transport.peer("batch-put-conflict-writer-a-segment"));
    let writer_b_transport = Arc::new(store_transport.peer("batch-put-conflict-writer-b-segment"));
    let reader_transport = Arc::new(store_transport.peer("batch-put-conflict-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store = StoreClientBuilder::new(metadata.clone(), "batch-put-conflict-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let writer_a = StoreClientBuilder::new(metadata.clone(), "batch-put-conflict-writer-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_a_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner.clone(), 1)
        .build(test_future_expiry_ms())
        .expect("writer-a build should succeed");
    let writer_b = StoreClientBuilder::new(metadata.clone(), "batch-put-conflict-writer-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_b_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer-b build should succeed");
    let reader = StoreClientBuilder::new(metadata, "batch-put-conflict-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    writer_a
        .register_local_memory()
        .expect("writer-a memory should register");
    writer_b
        .register_local_memory()
        .expect("writer-b memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store, &writer_a, &writer_b, &reader]);

    let mut source_a = b"batch-put-conflict-payload-a".to_vec();
    let mut source_b = b"batch-put-conflict-payload-b".to_vec();
    writer_a
        .register_buffer(source_a.as_mut_ptr().cast(), source_a.len())
        .expect("writer-a source buffer should register");
    writer_b
        .register_buffer(source_b.as_mut_ptr().cast(), source_b.len())
        .expect("writer-b source buffer should register");
    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false)
        .preferred_storage_owner(store.runtime_id().storage_key());

    blocking.arm_blocked_cas();
    std::thread::scope(|scope| {
        let writer_a_result = scope.spawn(|| {
            writer_a.batch_put_from(&[PutFromRequest::new(
                "batch-put-from-conflict-key",
                source_a.as_ptr().cast(),
                source_a.len(),
            )
            .replication(policy.clone())])
        });
        assert!(
            blocking.wait_until_blocked(Duration::from_secs(1)),
            "writer-a should reach the blocked route publish point"
        );
        let writer_b_result = scope.spawn(|| {
            writer_b.batch_put_from(&[PutFromRequest::new(
                "batch-put-from-conflict-key",
                source_b.as_ptr().cast(),
                source_b.len(),
            )
            .replication(policy.clone())])
        });
        assert!(
            blocking.wait_until_blocked_count(2, Duration::from_secs(1)),
            "both writers should reach the blocked route publish point"
        );
        blocking.release_blocked_cas();

        let routes_a = writer_a_result
            .join()
            .expect("writer-a thread should join")
            .expect("writer-a conflict loser should still succeed");
        let routes_b = writer_b_result
            .join()
            .expect("writer-b thread should join")
            .expect("writer-b conflict loser should still succeed");
        assert_eq!(routes_a.len(), 1);
        assert_eq!(routes_b.len(), 1);
        assert_eq!(routes_a[0].key, routes_b[0].key);
        assert_eq!(routes_a[0].version, routes_b[0].version);
    });

    let value = reader
        .get("batch-put-from-conflict-key")
        .expect("reader should fetch the published payload");
    assert!(
        value == source_a || value == source_b,
        "the authoritative route should point at one completed writer"
    );
}

#[test]
fn routed_batch_put_from_works_when_metadata_hot_paths_are_disabled() {
    let metadata = Arc::new(NoHotPathMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-hot-batch-from-segment"));
    let router_transport = Arc::new(storage_transport.peer("router-hot-batch-from-segment"));
    let reader_transport = Arc::new(storage_transport.peer("reader-hot-batch-from-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-hot-batch-from")
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

    let router = StoreClientBuilder::new(metadata.clone(), "router-hot-batch-from")
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

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-hot-batch-from")
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

    let mut source = [0u8; 32];
    source[..7].copy_from_slice(b"alpha-0");
    source[16..23].copy_from_slice(b"beta-00");
    router
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("router register buffer should succeed");

    let routes = router
        .batch_put_from(&[
            PutFromRequest::new("hot-batch-from-a", source.as_ptr().cast(), 7)
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
            PutFromRequest::new(
                "hot-batch-from-b",
                unsafe { source.as_ptr().add(16).cast() },
                7,
            )
            .tenant("tenant-a")
            .replication(ReplicationPolicy::new().prefer_local(false)),
        ])
        .expect("batch_put_from should succeed without metadata hot paths");
    assert_eq!(routes.len(), 2);
    for key in ["hot-batch-from-a", "hot-batch-from-b"] {
        assert!(
            metadata
                .inner
                .get_object_route(&ObjectKey::new(format!("tenant-a::{key}")))
                .expect("metadata query should succeed")
                .is_none(),
            "embedded WRH route directory should keep batch_put_from routes off metadata backend"
        );
    }

    let mut buf_a = [0u8; 7];
    let mut buf_b = [0u8; 7];
    let sizes = reader
        .batch_get_into(&mut [
            GetRequest::new("hot-batch-from-a", &mut buf_a).tenant("tenant-a"),
            GetRequest::new("hot-batch-from-b", &mut buf_b).tenant("tenant-a"),
        ])
        .expect("reader batch_get_into should succeed");
    assert_eq!(sizes, vec![7, 7]);
    assert_eq!(&buf_a, b"alpha-0");
    assert_eq!(&buf_b, b"beta-00");
}

#[test]
fn put_get_family_stays_off_backend_hot_path_after_build() {
    let metadata = Arc::new(HotPathBlockedMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-hot-family-segment"));
    let writer_transport = Arc::new(storage_transport.peer("writer-hot-family-segment"));
    let reader_transport = Arc::new(storage_transport.peer("reader-hot-family-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-hot-family")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-hot-family")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::ZERO)
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

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-hot-family")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::ZERO)
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    {
        let seeded_at_ms = now_ms();
        let mut cache = writer.live_client_cache.lock();
        for tenant in [
            "tenant-explicit",
            "tenant-policy",
            "tenant-batch",
            "tenant-from",
            "tenant-from-policy",
            "tenant-batch-from",
            "tenant-multi",
        ] {
            cache.store_tenant_quota_policy(tenant.to_string(), None, None, seeded_at_ms);
        }
    }

    metadata.block_hot_path();

    let policy = ReplicationPolicy::new().prefer_local(false);
    writer.put("plain", b"alpha").expect("put should succeed");
    writer
        .put_in_tenant("tenant-explicit", "tenant-key", b"bravo")
        .expect("put_in_tenant should succeed");
    writer
        .put_with_policy("policy", b"charlie", &policy)
        .expect("put_with_policy should succeed");
    writer
        .put_in_tenant_with_policy("tenant-policy", "policy-tenant", b"deltaaa", &policy)
        .expect("put_in_tenant_with_policy should succeed");
    writer
        .batch_put(&[
            PutRequest::new("batch-a", b"delta"),
            PutRequest::new("batch-b", b"echoo"),
            PutRequest::new("batch-tenant", b"foxten").tenant("tenant-batch"),
        ])
        .expect("batch_put should succeed");

    let mut source = [0u8; 160];
    source[..5].copy_from_slice(b"foxtt");
    source[16..22].copy_from_slice(b"golf!!");
    source[32..37].copy_from_slice(b"hotel");
    source[48..53].copy_from_slice(b"india");
    source[64..70].copy_from_slice(b"juliet");
    source[80..87].copy_from_slice(b"kilo123");
    source[96..103].copy_from_slice(b"mike999");
    writer
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("register_buffer should succeed");
    writer
        .put_from("from-one", source.as_ptr().cast(), 5)
        .expect("put_from should succeed");
    writer
        .put_from_in_tenant(
            "tenant-from",
            "from-tenant",
            unsafe { source.as_ptr().add(48).cast() },
            5,
        )
        .expect("put_from_in_tenant should succeed");
    writer
        .put_from_with_policy(
            "from-policy",
            unsafe { source.as_ptr().add(64).cast() },
            6,
            &policy,
        )
        .expect("put_from_with_policy should succeed");
    writer
        .put_from_in_tenant_with_policy(
            "tenant-from-policy",
            "from-tenant-policy",
            unsafe { source.as_ptr().add(80).cast() },
            7,
            &policy,
        )
        .expect("put_from_in_tenant_with_policy should succeed");
    writer
        .batch_put_from(&[
            PutFromRequest::new("from-batch-a", unsafe { source.as_ptr().add(16).cast() }, 6),
            PutFromRequest::new("from-batch-b", unsafe { source.as_ptr().add(32).cast() }, 5),
            PutFromRequest::new(
                "from-batch-tenant",
                unsafe { source.as_ptr().add(96).cast() },
                7,
            )
            .tenant("tenant-batch-from"),
        ])
        .expect("batch_put_from should succeed");
    let multi_buffers = [b"jul".as_slice(), b"iet".as_slice()];
    let tenant_multi_buffers = [b"nov".as_slice(), b"ember".as_slice()];
    writer
        .batch_put_from_multi_buffers(&[
            MultiBufferPutRequest::new("from-multi", &multi_buffers),
            MultiBufferPutRequest::new("from-multi-tenant", &tenant_multi_buffers)
                .tenant("tenant-multi")
                .replication(policy.clone()),
        ])
        .expect("batch_put_from_multi_buffers should succeed");

    assert_eq!(reader.get("plain").expect("get should succeed"), b"alpha");
    assert_eq!(
        reader
            .get_in_tenant("tenant-explicit", "tenant-key")
            .expect("get_in_tenant should succeed"),
        b"bravo"
    );
    assert!(
        reader.is_exist("policy").expect("is_exist should succeed"),
        "is_exist should report true"
    );
    assert!(
        reader
            .is_exist_in_tenant("tenant-policy", "policy-tenant")
            .expect("is_exist_in_tenant should succeed"),
        "is_exist_in_tenant should report true"
    );
    assert_eq!(
        reader
            .batch_is_exist(&[
                ObjectRef::new("plain"),
                ObjectRef::new("policy-tenant").tenant("tenant-policy"),
                ObjectRef::new("missing"),
            ])
            .expect("batch_is_exist should succeed"),
        vec![true, true, false]
    );
    assert_eq!(
        reader.get_size("policy").expect("get_size should succeed"),
        7
    );
    assert_eq!(
        reader
            .get_size_in_tenant("tenant-policy", "policy-tenant")
            .expect("get_size_in_tenant should succeed"),
        7
    );
    assert!(
        reader
            .query_route("plain")
            .expect("query_route should succeed")
            .is_some(),
        "query_route should see authoritative route"
    );
    let tenant_scope = NamespaceScope::with_defaults(Some("tenant-policy"), None, None);
    let tenant_policy_route = reader
        .query_route_in_tenant("tenant-policy", "policy-tenant")
        .expect("query_route_in_tenant should succeed")
        .expect("tenant route should exist");
    assert_eq!(
        tenant_policy_route.key,
        reader
            .query_route_in_scope(&tenant_scope, "policy-tenant")
            .expect("query_route_in_scope should succeed")
            .expect("scoped route should exist")
            .key
    );
    assert_eq!(
        tenant_policy_route.key,
        reader
            .query_route_by_object_id(&LogicalObjectId::new(tenant_scope.clone(), "policy-tenant",))
            .expect("query_route_by_object_id should succeed")
            .expect("object-id route should exist")
            .key
    );

    let mut single = [0u8; 7];
    assert_eq!(
        reader
            .get_into("policy", &mut single)
            .expect("get_into should succeed"),
        7
    );
    assert_eq!(&single, b"charlie");
    let mut tenant_single = [0u8; 8];
    assert_eq!(
        reader
            .get_into_in_tenant(
                "tenant-from-policy",
                "from-tenant-policy",
                &mut tenant_single
            )
            .expect("get_into_in_tenant should succeed"),
        7
    );
    assert_eq!(&tenant_single[..7], b"kilo123");

    let values = reader
        .batch_get(&[
            ObjectRef::new("batch-a"),
            ObjectRef::new("batch-b"),
            ObjectRef::new("batch-tenant").tenant("tenant-batch"),
            ObjectRef::new("from-one"),
            ObjectRef::new("from-tenant").tenant("tenant-from"),
            ObjectRef::new("from-policy"),
            ObjectRef::new("from-tenant-policy").tenant("tenant-from-policy"),
            ObjectRef::new("from-batch-a"),
            ObjectRef::new("from-batch-b"),
            ObjectRef::new("from-batch-tenant").tenant("tenant-batch-from"),
            ObjectRef::new("from-multi"),
            ObjectRef::new("from-multi-tenant").tenant("tenant-multi"),
        ])
        .expect("batch_get should succeed");
    assert_eq!(
        values,
        vec![
            b"delta".to_vec(),
            b"echoo".to_vec(),
            b"foxten".to_vec(),
            b"foxtt".to_vec(),
            b"india".to_vec(),
            b"juliet".to_vec(),
            b"kilo123".to_vec(),
            b"golf!!".to_vec(),
            b"hotel".to_vec(),
            b"mike999".to_vec(),
            b"juliet".to_vec(),
            b"november".to_vec(),
        ]
    );
    let buffered = reader
        .batch_get_buffer(&[
            ObjectRef::new("policy"),
            ObjectRef::new("policy-tenant").tenant("tenant-policy"),
            ObjectRef::new("from-policy"),
        ])
        .expect("batch_get_buffer should succeed");
    assert_eq!(buffered[0], b"charlie");
    assert_eq!(buffered[1], b"deltaaa");
    assert_eq!(buffered[2], b"juliet");

    let mut batch_a = [0u8; 5];
    let mut batch_tenant = [0u8; 7];
    let batch_sizes = reader
        .batch_get_into(&mut [
            GetRequest::new("batch-a", &mut batch_a),
            GetRequest::new("policy-tenant", &mut batch_tenant).tenant("tenant-policy"),
        ])
        .expect("batch_get_into should succeed");
    assert_eq!(batch_sizes, vec![5, 7]);
    assert_eq!(&batch_a, b"delta");
    assert_eq!(&batch_tenant, b"deltaaa");

    let mut multi_a = [0u8; 3];
    let mut multi_b = [0u8; 5];
    let mut shards = [&mut multi_a[..], &mut multi_b[..]];
    let multi_sizes = reader
        .batch_get_into_multi_buffers(&mut [MultiBufferGetRequest::new(
            "from-multi-tenant",
            &mut shards,
        )
        .tenant("tenant-multi")])
        .expect("batch_get_into_multi_buffers should succeed");
    assert_eq!(multi_sizes, vec![8]);
    assert_eq!(&multi_a, b"nov");
    assert_eq!(&multi_b, b"ember");
}

#[test]
fn batch_get_fairness_limits_remote_items_per_tenant_per_round() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-batch-fairness-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-batch-fairness-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
        "storage-batch-fairness",
        "seg-batch-fairness",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-fairness")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(writer_transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-fairness")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config_with_layout(4096, 64, 1))
        .execution_fairness(ExecutionFairness::new().max_remote_batch_items_per_tenant(1))
        .build(10_000)
        .expect("reader build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    writer
        .batch_put(&[
            PutRequest::new("fair-a1", b"aaaa")
                .tenant("tenant-a")
                .replication(policy.clone()),
            PutRequest::new("fair-a2", b"bbbb")
                .tenant("tenant-a")
                .replication(policy.clone()),
            PutRequest::new("fair-b1", b"cccc")
                .tenant("tenant-b")
                .replication(policy),
        ])
        .expect("remote batch put should succeed");

    let values = reader
        .batch_get(&[
            ObjectRef::new("fair-a1").tenant("tenant-a"),
            ObjectRef::new("fair-a2").tenant("tenant-a"),
            ObjectRef::new("fair-b1").tenant("tenant-b"),
        ])
        .expect("batch_get should honor fairness slicing");
    assert_eq!(
        values,
        vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]
    );

    let submitted = writer_transport.submitted_batch_sizes();
    assert!(submitted.windows(2).any(|window| window == [2, 1]));
}

#[test]
fn routed_put_fairness_limits_remote_replica_writes_per_batch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-put-fairness-segment"));
    let remote_a = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-put-fairness-a",
        "seg-put-fairness-a",
        "pool-a",
        1024,
        1,
    );
    let remote_b = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-put-fairness-b",
        "seg-put-fairness-b",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-put-fairness")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .execution_fairness(ExecutionFairness::new().max_remote_batch_items_per_tenant(1))
        .build(10_000)
        .expect("writer build should succeed");

    let route = writer
        .put_with_policy(
            "put-fairness",
            b"payload",
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owners([remote_a.storage_key(), remote_b.storage_key()]),
        )
        .expect("routed put should succeed");
    assert_eq!(route.replicas.len(), 2);

    let submitted = transport.submitted_batch_sizes();
    assert_eq!(submitted, vec![1, 1]);
}

#[test]
fn batch_get_shaping_caps_remote_batch_bytes() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-batch-shaping-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-batch-shaping-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
        "storage-batch-shaping",
        "seg-batch-shaping",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-shaping")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(writer_transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-shaping")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config_with_layout(4096, 64, 1))
        .bandwidth_shaping(BandwidthShaping::new().max_remote_batch_bytes(4))
        .build(10_000)
        .expect("reader build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    writer
        .batch_put(&[
            PutRequest::new("shape-a", b"aaaa").replication(policy.clone()),
            PutRequest::new("shape-b", b"bbbb").replication(policy.clone()),
            PutRequest::new("shape-c", b"cccc").replication(policy),
        ])
        .expect("remote batch put should succeed");

    let values = reader
        .batch_get(&[
            ObjectRef::new("shape-a"),
            ObjectRef::new("shape-b"),
            ObjectRef::new("shape-c"),
        ])
        .expect("batch_get should honor shaping limit");
    assert_eq!(
        values,
        vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]
    );

    let submitted = writer_transport.submitted_batch_bytes();
    assert!(submitted.iter().filter(|bytes| **bytes == 4).count() >= 3);
}

#[test]
fn batch_get_emits_transport_pacing_hints() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-batch-pacing-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-batch-pacing-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
        "storage-batch-pacing",
        "seg-batch-pacing",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-pacing")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(writer_transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-pacing")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config_with_layout(4096, 64, 1))
        .bandwidth_shaping(BandwidthShaping::new().max_inflight_bytes_per_batch(16))
        .build(10_000)
        .expect("reader build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    writer
        .put_with_policy("paced-get", b"payload", &policy)
        .expect("remote put should succeed");

    let value = reader.get("paced-get").expect("paced get should succeed");
    assert_eq!(value, b"payload");

    let hints = writer_transport.submitted_batch_hints();
    assert!(
        hints.iter().any(|hint| {
            hint.0.as_deref() == Some("tenant:default")
                && hint.1 == TransferPacingMode::LatencySensitive
                && hint.2 == Some(16)
        }),
        "hints: {hints:?}"
    );
}

#[test]
fn namespace_quota_isolated_per_scope() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("quota-scope-segment"));
    let client = StoreClientBuilder::new(metadata, "quota-scope-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config())
        .namespace_quota(NamespaceQuota::new().max_bytes(64).max_objects(4))
        .build(10_000)
        .expect("client build should succeed");

    let route_a = client
        .batch_put(&[PutRequest::new("key-a", b"alpha")
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")
            .qos_tier("gold")])
        .expect("scope-a write should succeed");
    assert_eq!(route_a.len(), 1);

    let route_b = client
        .batch_put(&[PutRequest::new("key-b", b"bravo")
            .tenant("tenant-a")
            .domain("domain-b")
            .object_set("set-b")
            .qos_tier("silver")])
        .expect("same tenant different scope should keep independent namespace quota state");
    assert_eq!(route_b.len(), 1);

    let other_tenant_route = client
        .batch_put(&[PutRequest::new("key-c", b"bravo")
            .tenant("tenant-b")
            .domain("domain-b")
            .object_set("set-b")
            .qos_tier("silver")])
        .expect("different tenant should have independent quota state");
    assert_eq!(other_tenant_route.len(), 1);

    let scope_a = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-a"), Some("set-a"));
    let scope_b = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-b"), Some("set-b"));
    let tenant_b_scope =
        NamespaceScope::with_defaults(Some("tenant-b"), Some("domain-b"), Some("set-b"));
    let scope_a_routes = client
        .list_routes_in_scope(&scope_a)
        .expect("scope-a listing should succeed");
    let scope_b_routes = client
        .list_routes_in_scope(&scope_b)
        .expect("scope-b listing should succeed");
    let tenant_b_routes = client
        .list_routes_in_scope(&tenant_b_scope)
        .expect("tenant-b scope listing should succeed");
    assert_eq!(scope_a_routes.len(), 1);
    assert_eq!(scope_a_routes[0].logical_key.as_deref(), Some("key-a"));
    assert_eq!(scope_b_routes.len(), 1);
    assert_eq!(scope_b_routes[0].logical_key.as_deref(), Some("key-b"));
    assert_eq!(tenant_b_routes.len(), 1);
    assert_eq!(tenant_b_routes[0].logical_key.as_deref(), Some("key-c"));
}

#[test]
fn namespace_scoped_placement_and_route_views_stay_isolated() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("namespace-isolation-segment"));
    publish_labeled_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-domain-a",
        "seg-domain-a",
        "pool-a",
        1024,
        1,
        Some("domain-a"),
        Some("set-a"),
        Some("gold"),
    );
    publish_labeled_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-domain-b",
        "seg-domain-b",
        "pool-a",
        1024,
        1,
        Some("domain-b"),
        Some("set-b"),
        Some("silver"),
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "namespace-isolation-writer")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("writer build should succeed");

    let routes = writer
        .batch_put(&[
            PutRequest::new("key-a", b"payload-a")
                .tenant("tenant-a")
                .domain("domain-a")
                .object_set("set-a")
                .qos_tier("gold")
                .replication(
                    ReplicationPolicy::new()
                        .replica_count(1)
                        .prefer_local(false),
                ),
            PutRequest::new("key-b", b"payload-b")
                .tenant("tenant-a")
                .domain("domain-b")
                .object_set("set-b")
                .qos_tier("silver")
                .replication(
                    ReplicationPolicy::new()
                        .replica_count(1)
                        .prefer_local(false),
                ),
        ])
        .expect("scoped writes should succeed");
    assert_eq!(routes.len(), 2);

    let route_a = routes
        .iter()
        .find(|route| route.logical_key.as_deref() == Some("key-a"))
        .expect("route-a should exist");
    let route_b = routes
        .iter()
        .find(|route| route.logical_key.as_deref() == Some("key-b"))
        .expect("route-b should exist");
    assert_eq!(route_a.replicas[0].owner.stable_id.0, "storage-domain-a");
    assert_eq!(route_b.replicas[0].owner.stable_id.0, "storage-domain-b");
    assert_eq!(
        route_a.namespace.as_ref(),
        Some(&NamespaceScope::with_defaults(
            Some("tenant-a"),
            Some("domain-a"),
            Some("set-a")
        ))
    );
    assert_eq!(
        route_b.namespace.as_ref(),
        Some(&NamespaceScope::with_defaults(
            Some("tenant-a"),
            Some("domain-b"),
            Some("set-b")
        ))
    );
    assert_eq!(route_a.qos_tier.as_deref(), Some("gold"));
    assert_eq!(route_b.qos_tier.as_deref(), Some("silver"));

    let queried_a = writer
        .query_route_by_object_id(&LogicalObjectId::new(
            NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-a"), Some("set-a")),
            "key-a",
        ))
        .expect("route-a query should succeed")
        .expect("route-a should exist");
    let queried_b = writer
        .query_route_by_object_id(&LogicalObjectId::new(
            NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-b"), Some("set-b")),
            "key-b",
        ))
        .expect("route-b query should succeed")
        .expect("route-b should exist");
    assert_eq!(queried_a.namespace, route_a.namespace);
    assert_eq!(queried_b.namespace, route_b.namespace);

    let scope_a = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-a"), Some("set-a"));
    let scope_b = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-b"), Some("set-b"));
    let scope_a_routes = writer
        .list_routes_in_scope(&scope_a)
        .expect("scope-a listing should succeed");
    let scope_b_routes = writer
        .list_routes_in_scope(&scope_b)
        .expect("scope-b listing should succeed");
    assert_eq!(scope_a_routes.len(), 1);
    assert_eq!(scope_a_routes[0].logical_key.as_deref(), Some("key-a"));
    assert_eq!(scope_b_routes.len(), 1);
    assert_eq!(scope_b_routes[0].logical_key.as_deref(), Some("key-b"));

    let reuse_a = writer
        .list_reuse_candidates(&mooncake_store_core::ReuseIdentity::new(
            "tenant-a",
            "domain-a",
            "tenant-a",
            "tenant-a/domain-a/set-a/key-a",
        ))
        .expect("reuse-a listing should succeed");
    let reuse_b = writer
        .list_reuse_candidates(&mooncake_store_core::ReuseIdentity::new(
            "tenant-a",
            "domain-b",
            "tenant-a",
            "tenant-a/domain-b/set-b/key-b",
        ))
        .expect("reuse-b listing should succeed");
    assert_eq!(reuse_a.len(), 1);
    assert_eq!(reuse_a[0].logical_key.as_deref(), Some("key-a"));
    assert_eq!(reuse_b.len(), 1);
    assert_eq!(reuse_b[0].logical_key.as_deref(), Some("key-b"));
}

#[test]
fn same_tenant_scoped_logical_key_coexists_across_domains() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("same-logical-key-segment"));
    let client = StoreClientBuilder::new(metadata, "same-logical-key-client")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("client build should succeed");

    let routes = client
        .batch_put(&[
            PutRequest::new("shared-key", b"payload-a")
                .tenant("tenant-a")
                .domain("domain-a")
                .object_set("set-a"),
            PutRequest::new("shared-key", b"payload-b")
                .tenant("tenant-a")
                .domain("domain-b")
                .object_set("set-b"),
        ])
        .expect("same logical key in different scopes should coexist");
    assert_eq!(routes.len(), 2);
    assert_ne!(routes[0].key, routes[1].key);

    let scope_a = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-a"), Some("set-a"));
    let scope_b = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-b"), Some("set-b"));
    assert_eq!(
        client
            .query_route_by_object_id(&LogicalObjectId::new(scope_a.clone(), "shared-key"))
            .expect("scope-a query should succeed")
            .expect("scope-a route should exist")
            .canonical_key
            .as_deref(),
        Some("tenant-a/domain-a/set-a/shared-key")
    );
    assert_eq!(
        client
            .query_route_by_object_id(&LogicalObjectId::new(scope_b.clone(), "shared-key"))
            .expect("scope-b query should succeed")
            .expect("scope-b route should exist")
            .canonical_key
            .as_deref(),
        Some("tenant-a/domain-b/set-b/shared-key")
    );

    let payloads = client
        .batch_get(&[
            ObjectRef::new("shared-key")
                .tenant("tenant-a")
                .domain("domain-a")
                .object_set("set-a"),
            ObjectRef::new("shared-key")
                .tenant("tenant-a")
                .domain("domain-b")
                .object_set("set-b"),
        ])
        .expect("scoped gets should succeed");
    assert_eq!(payloads, vec![b"payload-a".to_vec(), b"payload-b".to_vec()]);

    let scope_a_routes = client
        .list_routes_in_scope(&scope_a)
        .expect("scope-a listing should succeed");
    let scope_b_routes = client
        .list_routes_in_scope(&scope_b)
        .expect("scope-b listing should succeed");
    assert_eq!(scope_a_routes.len(), 1);
    assert_eq!(scope_b_routes.len(), 1);
    assert_eq!(scope_a_routes[0].key, routes[0].key);
    assert_eq!(scope_b_routes[0].key, routes[1].key);

    client
        .batch_remove(
            &[ObjectRef::new("shared-key")
                .tenant("tenant-a")
                .domain("domain-a")
                .object_set("set-a")],
            false,
        )
        .expect("scoped remove should succeed");
    assert!(client
        .query_route_by_object_id(&LogicalObjectId::new(scope_a, "shared-key"))
        .expect("scope-a query after remove should succeed")
        .is_none());
    assert_eq!(
        client
            .batch_get(&[ObjectRef::new("shared-key")
                .tenant("tenant-a")
                .domain("domain-b")
                .object_set("set-b")])
            .expect("scope-b get should still succeed"),
        vec![b"payload-b".to_vec()]
    );
}

#[test]
fn put_paths_accept_namespace_dimensions_beyond_routed_fast_path() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-namespace-placement-segment"));
    publish_labeled_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-domain-a",
        "seg-domain-a",
        "pool-a",
        1024,
        1,
        Some("domain-a"),
        Some("set-a"),
        Some("gold"),
    );
    publish_labeled_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-domain-b",
        "seg-domain-b",
        "pool-a",
        1024,
        1,
        Some("domain-b"),
        Some("set-b"),
        Some("silver"),
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-namespace-placement")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("writer build should succeed");

    let batch_route = writer
        .batch_put(&[PutRequest::new("namespace-key", b"payload")
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")
            .qos_tier("gold")
            .replication(
                ReplicationPolicy::new()
                    .replica_count(1)
                    .prefer_local(false),
            )])
        .expect("batch put should accept namespace dimensions");
    assert_eq!(batch_route.len(), 1);
    assert_eq!(
        batch_route[0].replicas[0].owner.stable_id.0,
        "storage-domain-a"
    );
    assert_eq!(
        batch_route[0].namespace.as_ref(),
        Some(&NamespaceScope::with_defaults(
            Some("tenant-a"),
            Some("domain-a"),
            Some("set-a"),
        ))
    );
    assert_eq!(batch_route[0].qos_tier.as_deref(), Some("gold"));

    let mut source = [0u8; 16];
    source[..6].copy_from_slice(b"direct");
    writer
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("buffer register should succeed");

    let from_route = writer
        .batch_put_from(&[
            PutFromRequest::new("namespace-from", source.as_ptr().cast(), 6)
                .tenant("tenant-a")
                .domain("domain-a")
                .object_set("set-a")
                .qos_tier("gold")
                .replication(
                    ReplicationPolicy::new()
                        .replica_count(1)
                        .prefer_local(false),
                ),
        ])
        .expect("batch put_from should accept namespace dimensions");
    assert_eq!(from_route.len(), 1);
    assert_eq!(
        from_route[0].replicas[0].owner.stable_id.0,
        "storage-domain-a"
    );
    assert_eq!(
        from_route[0].namespace.as_ref(),
        Some(&NamespaceScope::with_defaults(
            Some("tenant-a"),
            Some("domain-a"),
            Some("set-a"),
        ))
    );
    assert_eq!(from_route[0].qos_tier.as_deref(), Some("gold"));

    let slices = [b"multi".as_slice(), b"-gold".as_slice()];
    let multi_route = writer
        .batch_put_from_multi_buffers(&[MultiBufferPutRequest::new("namespace-multi", &slices)
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")
            .qos_tier("gold")
            .replication(
                ReplicationPolicy::new()
                    .replica_count(1)
                    .prefer_local(false),
            )])
        .expect("multi-buffer put should accept namespace dimensions");
    assert_eq!(multi_route.len(), 1);
    assert_eq!(
        multi_route[0].replicas[0].owner.stable_id.0,
        "storage-domain-a"
    );
    assert_eq!(
        multi_route[0].namespace.as_ref(),
        Some(&NamespaceScope::with_defaults(
            Some("tenant-a"),
            Some("domain-a"),
            Some("set-a"),
        ))
    );
    assert_eq!(multi_route[0].qos_tier.as_deref(), Some("gold"));

    writer
        .unregister_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("buffer unregister should succeed");
}

#[test]
fn routed_put_emits_transport_pacing_hints() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-put-pacing-segment"));
    let remote_a = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-put-pacing-a",
        "seg-put-pacing-a",
        "pool-a",
        1024,
        1,
    );
    let remote_b = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-put-pacing-b",
        "seg-put-pacing-b",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-put-pacing")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .bandwidth_shaping(BandwidthShaping::new().max_inflight_bytes_per_batch(9))
        .build(10_000)
        .expect("writer build should succeed");

    writer
        .put_with_policy(
            "paced-put",
            b"payload",
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owners([remote_a.storage_key(), remote_b.storage_key()]),
        )
        .expect("routed put should succeed");

    let hints = transport.submitted_batch_hints();
    assert!(
        hints.iter().any(|hint| {
            hint.0.as_deref() == Some("tenant:default")
                && hint.1 == TransferPacingMode::ThroughputOptimized
                && hint.2 == Some(9)
        }),
        "hints: {hints:?}"
    );
}

#[test]
fn routed_batch_put_emits_transport_pacing_hints() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-batch-put-pacing-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-batch-put-pacing",
        "seg-batch-put-pacing",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-batch-put-pacing")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .bandwidth_shaping(BandwidthShaping::new().max_inflight_bytes_per_batch(11))
        .build(10_000)
        .expect("writer build should succeed");

    writer
        .batch_put(&[
            PutRequest::new("paced-a", b"aaaa")
                .tenant("tenant-a")
                .replication(
                    ReplicationPolicy::new()
                        .prefer_local(false)
                        .preferred_storage_owner(remote_owner.storage_key()),
                ),
            PutRequest::new("paced-b", b"bbbb")
                .tenant("tenant-b")
                .replication(
                    ReplicationPolicy::new()
                        .prefer_local(false)
                        .preferred_storage_owner(remote_owner.storage_key()),
                ),
        ])
        .expect("batch routed put should succeed");

    let hints = transport.submitted_batch_hints();
    assert!(
        hints.iter().any(|hint| {
            hint.0.as_deref() == Some("tenant:tenant-a")
                && hint.1 == TransferPacingMode::ThroughputOptimized
                && hint.2 == Some(11)
        }),
        "hints: {hints:?}"
    );
    assert!(
        hints.iter().any(|hint| {
            hint.0.as_deref() == Some("tenant:tenant-b")
                && hint.1 == TransferPacingMode::ThroughputOptimized
                && hint.2 == Some(11)
        }),
        "hints: {hints:?}"
    );
}

#[test]
fn routed_batch_put_coalesces_remote_writes_by_tenant() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-batch-coalesce-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-batch-coalesce",
        "seg-batch-coalesce",
        "pool-a",
        4096,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-batch-coalesce")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("writer build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    writer
        .batch_put(&[
            PutRequest::new("coalesce-a", b"aaaa")
                .tenant("tenant-a")
                .replication(policy.clone()),
            PutRequest::new("coalesce-b", b"bbbb")
                .tenant("tenant-a")
                .replication(policy.clone()),
            PutRequest::new("coalesce-c", b"cccc")
                .tenant("tenant-a")
                .replication(policy),
        ])
        .expect("remote batch put should succeed");

    let submitted = transport.submitted_batch_sizes();
    assert!(submitted.contains(&3), "submitted batches: {submitted:?}");
}

#[test]
fn routed_batch_put_chunks_by_scratch_window() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-batch-scratch-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-batch-scratch",
        "seg-batch-scratch",
        "pool-a",
        4096,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-batch-scratch")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config_with_layout(4096, 8, 1))
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("writer build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    writer
        .batch_put(&[
            PutRequest::new("scratch-a", b"aaaa")
                .tenant("tenant-a")
                .replication(policy.clone()),
            PutRequest::new("scratch-b", b"bbbb")
                .tenant("tenant-a")
                .replication(policy.clone()),
            PutRequest::new("scratch-c", b"cccc")
                .tenant("tenant-a")
                .replication(policy),
        ])
        .expect("remote batch put should chunk by scratch capacity");

    let submitted = transport.submitted_batch_sizes();
    assert!(
        submitted.windows(2).any(|window| window == [2, 1]),
        "submitted batches: {submitted:?}"
    );
}

#[test]
fn routed_put_shaping_caps_remote_batch_bytes() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-put-shaping-segment"));
    let remote_a = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-put-shaping-a",
        "seg-put-shaping-a",
        "pool-a",
        1024,
        1,
    );
    let remote_b = publish_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-put-shaping-b",
        "seg-put-shaping-b",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-put-shaping")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .bandwidth_shaping(BandwidthShaping::new().max_remote_batch_bytes(7))
        .build(10_000)
        .expect("writer build should succeed");

    writer
        .put_with_policy(
            "put-shaping",
            b"payload",
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owners([remote_a.storage_key(), remote_b.storage_key()]),
        )
        .expect("routed put should succeed");

    let submitted = transport.submitted_batch_bytes();
    assert_eq!(submitted, vec![7, 7]);
}

#[test]
fn batch_get_chunks_remote_reads_when_scratch_window_is_small() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-batch-chunk-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-batch-chunk-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
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
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-chunk")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
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
    let writer_transport = Arc::new(TestTransport::new("writer-batch-direct-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-batch-direct-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
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
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-direct")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
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
fn batch_get_into_batches_registered_remote_buffers_without_scratch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-batch-registered-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-batch-registered-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
        "storage-batch-registered",
        "seg-batch-registered",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-registered")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-registered")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config_with_layout(4096, 4, 1))
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

    writer
        .batch_put(&[
            PutRequest::new("registered-a", b"abcdefgh").replication(
                ReplicationPolicy::new()
                    .prefer_local(false)
                    .preferred_storage_owner(remote_owner.storage_key()),
            ),
            PutRequest::new("registered-b", b"ijklmnop").replication(
                ReplicationPolicy::new()
                    .prefer_local(false)
                    .preferred_storage_owner(remote_owner.storage_key()),
            ),
        ])
        .expect("remote batch put should succeed");

    let mut target = [0u8; 32];
    reader
        .register_buffer(target.as_mut_ptr().cast(), target.len())
        .expect("reader register buffer should succeed");
    {
        let mut state = writer_transport.state.lock();
        state.submitted_batch_sizes.clear();
        state.submitted_batch_bytes.clear();
        state.submitted_batch_hints.clear();
        state.submitted_request_sources.clear();
        state.submitted_request_opcodes.clear();
    }

    let (first, tail) = target.split_at_mut(8);
    let (_, second) = tail.split_at_mut(8);
    let sizes = reader
        .batch_get_into(&mut [
            GetRequest::new("registered-a", first),
            GetRequest::new("registered-b", &mut second[..8]),
        ])
        .expect("registered buffers should batch direct remote reads");
    assert_eq!(sizes, vec![8, 8]);
    assert_eq!(&target[..8], b"abcdefgh");
    assert_eq!(&target[16..24], b"ijklmnop");
    assert_eq!(writer_transport.submitted_batch_sizes(), vec![2]);
    assert_eq!(writer_transport.submitted_batch_bytes(), vec![16]);
    assert_eq!(
        writer_transport.submitted_request_sources(),
        vec![vec![target.as_mut_ptr() as usize, unsafe {
            target.as_mut_ptr().add(16) as usize
        },]]
    );

    let opcodes = writer_transport.submitted_request_opcodes();
    assert_eq!(opcodes.len(), 1);
    assert!(opcodes[0].iter().all(|opcode| *opcode == Opcode::Read));
}

#[test]
fn batch_put_from_uses_registered_remote_sources_without_scratch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-batch-put-from-segment"));
    writer_transport.set_max_registration_bytes(Some(4));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
        "storage-batch-put-from",
        "seg-batch-put-from",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata, "writer-batch-put-from")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport.clone())
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    wait_for_membership_convergence(&[&writer]);

    let mut source = [0u8; 32];
    source[..8].copy_from_slice(b"abcdefgh");
    source[16..24].copy_from_slice(b"ijklmnop");
    writer
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("writer register buffer should succeed");
    {
        let mut state = writer_transport.state.lock();
        state.submitted_batch_sizes.clear();
        state.submitted_batch_bytes.clear();
        state.submitted_batch_hints.clear();
        state.submitted_request_sources.clear();
        state.submitted_request_opcodes.clear();
    }

    let routes = writer
        .batch_put_from(&[
            PutFromRequest::new("remote-a", source.as_ptr().cast(), 8).replication(
                ReplicationPolicy::new()
                    .prefer_local(false)
                    .preferred_storage_owner(remote_owner.storage_key()),
            ),
            PutFromRequest::new("remote-b", unsafe { source.as_ptr().add(16).cast() }, 8)
                .replication(
                    ReplicationPolicy::new()
                        .prefer_local(false)
                        .preferred_storage_owner(remote_owner.storage_key()),
                ),
        ])
        .expect("batch_put_from should use registered remote sources");
    assert_eq!(routes.len(), 2);
    assert_eq!(writer_transport.submitted_batch_sizes(), vec![4]);
    assert_eq!(writer_transport.submitted_batch_bytes(), vec![16]);
    assert_eq!(
        writer_transport.submitted_request_sources(),
        vec![vec![
            source.as_mut_ptr() as usize,
            unsafe { source.as_mut_ptr().add(4) as usize },
            unsafe { source.as_mut_ptr().add(16) as usize },
            unsafe { source.as_mut_ptr().add(20) as usize },
        ]]
    );
    let opcodes = writer_transport.submitted_request_opcodes();
    assert_eq!(opcodes.len(), 1);
    assert!(opcodes[0].iter().all(|opcode| *opcode == Opcode::Write));
}

#[test]
fn local_only_batch_put_from_uses_registered_batch_write_path() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new(
        "local-only-writer-batch-put-from-segment",
    ));
    writer_transport.set_max_registration_bytes(Some(4));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &writer_transport,
        "local-only-storage-batch-put-from",
        "local-only-seg-batch-put-from",
        "pool-a",
        1024,
        1,
    );
    let mut writer = StoreClientBuilder::new(metadata, "local-only-writer-batch-put-from")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    wait_for_membership_convergence(&[&writer]);
    let route_directory = Arc::new(CountingRouteDirectory::new(writer.route_directory.clone()));
    writer.route_directory = route_directory.clone();

    let mut source = [0u8; 32];
    source[..8].copy_from_slice(b"abcdefgh");
    source[16..24].copy_from_slice(b"ijklmnop");
    writer
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("writer register buffer should succeed");
    {
        let mut state = writer_transport.state.lock();
        state.submitted_batch_sizes.clear();
        state.submitted_batch_bytes.clear();
        state.submitted_batch_hints.clear();
        state.submitted_request_sources.clear();
        state.submitted_request_opcodes.clear();
    }

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    let routes = writer
        .batch_put_from(&[
            PutFromRequest::new("local-only-remote-a", source.as_ptr().cast(), 8)
                .replication(policy.clone()),
            PutFromRequest::new(
                "local-only-remote-b",
                unsafe { source.as_ptr().add(16).cast() },
                8,
            )
            .replication(policy),
        ])
        .expect("local-only batch_put_from should use registered remote sources");
    assert_eq!(routes.len(), 2);
    assert_eq!(writer_transport.submitted_batch_bytes(), vec![16]);
    assert_eq!(
        writer_transport.submitted_request_sources(),
        vec![vec![
            source.as_mut_ptr() as usize,
            unsafe { source.as_mut_ptr().add(4) as usize },
            unsafe { source.as_mut_ptr().add(16) as usize },
            unsafe { source.as_mut_ptr().add(20) as usize },
        ]]
    );
    let opcodes = writer_transport.submitted_request_opcodes();
    assert_eq!(opcodes.len(), 1);
    assert!(opcodes[0].iter().all(|opcode| *opcode == Opcode::Write));
    assert_eq!(
        route_directory.get_object_routes_calls(),
        0,
        "accept-existing batch_put_from should skip the publish-stage route preload"
    );

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"batch_put_stage_write\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_route_cas\",status=\"ok\""));
}

#[test]
fn registered_buffer_subranges_support_put_from_and_batch_get_into() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-subrange-segment"));
    let reader_transport = Arc::new(writer_transport.peer("reader-subrange-segment"));
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-subrange")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-subrange")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
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
fn qos_tier_drives_namespace_governance_and_placement() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("qos-governance-segment"));
    publish_labeled_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-gold",
        "seg-gold",
        "pool-a",
        1024,
        1,
        Some("domain-a"),
        Some("set-a"),
        Some("gold"),
    );
    publish_labeled_storage_node_with_capacity(
        &metadata,
        &transport,
        "storage-bronze",
        "seg-bronze",
        "pool-a",
        1024,
        1,
        Some("domain-a"),
        Some("set-a"),
        Some("bronze"),
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let client = StoreClientBuilder::new(metadata.clone(), "qos-governance-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config())
        .namespace_quota(NamespaceQuota::new().max_bytes(8).max_objects(2))
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("client build should succeed");

    let gold = client
        .batch_put(&[PutRequest::new("gold-key", b"1234")
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")
            .qos_tier("gold")
            .replication(
                ReplicationPolicy::new()
                    .replica_count(1)
                    .prefer_local(false),
            )])
        .expect("gold write should succeed");
    let bronze = client
        .batch_put(&[PutRequest::new("bronze-key", b"5678")
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")
            .qos_tier("bronze")
            .replication(
                ReplicationPolicy::new()
                    .replica_count(1)
                    .prefer_local(false),
            )])
        .expect("bronze write should succeed");

    assert_eq!(gold[0].replicas[0].owner.stable_id.0, "storage-gold");
    assert_eq!(gold[0].qos_tier.as_deref(), Some("gold"));
    assert_eq!(bronze[0].qos_tier.as_deref(), Some("bronze"));

    let scope = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-a"), Some("set-a"));
    let routes = client
        .list_routes_in_scope(&scope)
        .expect("scope listing should succeed");
    assert_eq!(routes.len(), 2);
    assert!(routes
        .iter()
        .any(|route| route.logical_key.as_deref() == Some("gold-key")));
    assert!(routes
        .iter()
        .any(|route| route.logical_key.as_deref() == Some("bronze-key")));

    let overflow = client
        .batch_put(&[PutRequest::new("gold-overflow", b"x")
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")
            .qos_tier("gold")])
        .expect("shared namespace quota should evict within the scope regardless of qos tier");
    assert_eq!(overflow.len(), 1);
    assert_eq!(overflow[0].qos_tier.as_deref(), Some("gold"));

    let routes_after = client
        .list_routes_in_scope(&scope)
        .expect("scope listing after overflow should succeed");
    assert_eq!(routes_after.len(), 2);
    assert!(routes_after
        .iter()
        .any(|route| route.logical_key.as_deref() == Some("gold-overflow")));

    let quota = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load")
        .expect("quota state should exist");
    assert!(quota.used_bytes <= 8);
    assert!(quota.used_objects <= 2);
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
                .numa_aware(false)
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
    let owner_a = publish_storage_node(&metadata, &transport, "storage-a", "seg-a", "pool-a");
    let owner_b = publish_storage_node(&metadata, &transport, "storage-b", "seg-b", "pool-a");
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
        &metadata,
        &transport,
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
fn query_route_marks_local_replica_hot_for_eviction() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("query-hot-local-segment"));
    let client = StoreClientBuilder::new(metadata, "query-hot-local")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(32))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    client
        .put("query-hot", b"0123456789abcdef")
        .expect("hot put should succeed");
    client
        .put("query-cold", b"fedcba9876543210")
        .expect("cold put should succeed");
    assert!(client
        .query_route("query-hot")
        .expect("query route should succeed")
        .is_some());

    client
        .put("query-fresh", b"abcdefghijklmnop")
        .expect("fresh put should trigger eviction and succeed");
    assert!(client
        .query_route("query-hot")
        .expect("hot route query should succeed")
        .is_some());
    assert!(client
        .query_route("query-fresh")
        .expect("fresh route query should succeed")
        .is_some());
    assert!(client
        .query_route("query-cold")
        .expect("cold route query should succeed")
        .is_none());
}

#[test]
fn remote_hit_reports_drive_storage_owner_clock_eviction() {
    let _guard = metrics_test_lock().lock();
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
    wait_for_storage_clock_route(&storage, &hot_route, false);
    wait_for_storage_clock_route(&storage, &cold_route, false);
    assert_eq!(
        router
            .get("remote-clock-hot")
            .expect("remote hot get should succeed"),
        hot
    );
    wait_for_storage_clock_hot(&storage, &hot_route);

    let fresh_route = router
        .put("remote-clock-fresh", fresh)
        .expect("fresh put should trigger remote eviction and succeed");
    assert_eq!(fresh_route.replicas[0].owner, storage.runtime_id().clone());
    assert!(router
        .query_route("remote-clock-fresh")
        .expect("fresh route query should succeed")
        .is_some());
    assert_eq!(
        router
            .get("remote-clock-fresh")
            .expect("fresh object should be readable"),
        fresh
    );
    assert_eq!(
        router
            .get("remote-clock-hot")
            .expect("hot object should remain readable"),
        hot
    );
    assert!(matches!(
        router.get("remote-clock-cold"),
        Err(StoreError::NotFound(_))
    ));
}

#[test]
fn remote_get_marks_all_readable_replicas_hot_for_eviction() {
    let _guard = metrics_test_lock().lock();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("clock-replicated-store-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("clock-replicated-store-b-segment"));
    let router_transport = Arc::new(store_a_transport.peer("clock-replicated-router-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "clock-replicated-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "clock-replicated-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "clock-replicated-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");

    wait_for_membership_convergence(&[&store_a, &store_b, &router]);

    let payload = b"replicated-hot-payload";
    let route = router
        .put_with_policy(
            "replicated-hot",
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
    assert_eq!(route.replicas.len(), 2);
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == *store_a.runtime_id()));
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == *store_b.runtime_id()));

    assert_eq!(
        router
            .get("replicated-hot")
            .expect("remote get should succeed"),
        payload
    );

    wait_for_storage_clock_hot(&store_a, &route);
    wait_for_storage_clock_hot(&store_b, &route);
}

#[test]
fn get_marks_route_hot_before_payload_validation() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let storage_transport = Arc::new(TestTransport::new("get-early-hot-storage-segment"));
    let router_transport = Arc::new(storage_transport.peer("get-early-hot-router-segment"));
    let storage = StoreClientBuilder::new(metadata.clone(), "get-early-hot-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config_with_bytes(32))
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "get-early-hot-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    wait_for_membership_convergence(&[&storage, &router]);

    let hot_route = router
        .put("early-hot", b"0123456789abcdef")
        .expect("hot put should succeed");
    let cold_route = router
        .put("early-cold", b"fedcba9876543210")
        .expect("cold put should succeed");
    wait_for_storage_clock_route(&storage, &hot_route, false);
    wait_for_storage_clock_route(&storage, &cold_route, false);

    let current_hot_route = router
        .query_route("early-hot")
        .expect("hot route query should succeed")
        .expect("hot route should exist");
    assert_eq!(
        current_hot_route.replicas[0].owner,
        storage.runtime_id().clone()
    );
    let mut bad_route = current_hot_route.clone();
    bad_route.version = current_hot_route.version.next();
    bad_route.replicas[0].checksum = Some(payload_checksum(b"wrong-payload"));
    let cas = router
        .route_directory
        .compare_and_swap_object_route(
            &router.lease,
            &bad_route.key,
            Some(current_hot_route.version),
            Some(&bad_route),
        )
        .expect("bad checksum route publish should succeed");
    assert!(cas.applied, "bad checksum route publish should apply");

    let error = router
        .get("early-hot")
        .expect_err("checksum mismatch should fail the get after route resolution");
    assert!(
        error.to_string().contains("checksum"),
        "expected checksum failure, got {error}"
    );
    wait_for_storage_clock_hot(&storage, &bad_route);

    router
        .put("early-fresh", b"abcdefghijklmnop")
        .expect("fresh put should trigger eviction and succeed");
    assert!(router
        .query_route("early-hot")
        .expect("hot route query should succeed")
        .is_some());
    assert!(router
        .query_route("early-fresh")
        .expect("fresh route query should succeed")
        .is_some());
    assert!(router
        .query_route("early-cold")
        .expect("cold route query should succeed")
        .is_none());
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
    let remote_transport = Arc::new(transport.peer("seg-prefer"));
    remote_transport.add_external_segment("seg-prefer", 8);
    let remote_target_chunks = test_segment_target_chunks(&remote_transport, "seg-prefer");
    let remote_owner = ClientRuntimeId::new("storage-prefer", ClientEpoch(1));
    let remote_allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
    remote_allocator.lock().upsert(&SegmentAnnouncement {
        owner: remote_owner.clone(),
        segment_name: SegmentName::new("seg-prefer"),
        transport_endpoint: None,
        transport_segment_descriptor: None,
        capacity_bytes: 8,
        used_bytes: 0,
        target_chunks: remote_target_chunks.clone(),
        state: SegmentLifecycleState::Active,
        alignment_bytes: 1,
        tags: vec!["dram".to_string()],
    });
    let remote_control = ControlPlaneHandle::spawn(
        "127.0.0.1",
        Arc::new(LocalAuthorityAdapter {
            lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
            route_write_gate: test_route_write_gate(),
        }),
        Arc::new(StaticAllocatorAdapter {
            runtime: remote_owner.clone(),
            allocator: remote_allocator,
        }),
        Arc::new(NoopEvictionService),
    )
    .expect("static storage control plane should spawn");
    let mut endpoints = ClientEndpointSet {
        rpc_address: "127.0.0.1:0".to_string(),
        segment_name: Some(SegmentName::new("seg-prefer")),
        labels: Default::default(),
    };
    endpoints
        .labels
        .insert("pool".to_string(), "pool-a".to_string());
    endpoints
        .labels
        .insert("storage".to_string(), "false".to_string());
    endpoints
        .labels
        .insert("route".to_string(), "false".to_string());
    endpoints.labels.insert(
        control_address_label().to_string(),
        remote_control.address().to_string(),
    );
    metadata
        .upsert_client_lease(&ClientLease {
            runtime: remote_owner.clone(),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints,
            expires_at_ms: test_future_expiry_ms(),
        })
        .expect("static storage lease should upsert");
    metadata
        .publish_segment(&SegmentAnnouncement {
            owner: remote_owner.clone(),
            segment_name: SegmentName::new("seg-prefer"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 8,
            used_bytes: 0,
            target_chunks: remote_target_chunks,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: vec!["dram".to_string()],
        })
        .expect("static storage segment should publish");
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
    drop(remote_control);
}

#[test]
fn routed_batch_put_prefers_local_before_spilling_remote() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("router-local-first-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        &metadata,
        &transport,
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
        &metadata,
        &transport,
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
    let owner_b = publish_storage_node(&metadata, &transport, "storage-b", "seg-b", "pool-a");
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

    let object = ObjectRef::new("alpha")
        .tenant("tenant-a")
        .domain("domain-a")
        .object_set("set-a")
        .qos_tier("gold");
    assert_eq!(object.tenant, Some("tenant-a"));
    assert_eq!(object.domain, Some("domain-a"));
    assert_eq!(object.object_set, Some("set-a"));
    assert_eq!(object.qos_tier, Some("gold"));
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
        .domain("domain-a")
        .object_set("set-a")
        .qos_tier("gold")
        .replication(policy.clone());
    assert_eq!(put.tenant, Some("tenant-a"));
    assert_eq!(put.domain, Some("domain-a"));
    assert_eq!(put.object_set, Some("set-a"));
    assert_eq!(put.qos_tier, Some("gold"));
    assert_eq!(put.key, "put-key");
    assert_eq!(put.value, b"put-value");
    assert_eq!(put.policy, Some(policy.clone()));

    let source = [1u8, 2, 3, 4];
    let put_from = PutFromRequest::new("from-key", source.as_ptr().cast(), source.len())
        .tenant("tenant-a")
        .domain("domain-a")
        .object_set("set-a")
        .qos_tier("gold")
        .replication(policy.clone());
    assert_eq!(put_from.tenant, Some("tenant-a"));
    assert_eq!(put_from.domain, Some("domain-a"));
    assert_eq!(put_from.object_set, Some("set-a"));
    assert_eq!(put_from.qos_tier, Some("gold"));
    assert_eq!(put_from.key, "from-key");
    assert_eq!(put_from.size, source.len());
    assert_eq!(put_from.policy, Some(policy.clone()));

    let multi_put_buffers = [b"left".as_slice(), b"-right".as_slice()];
    let multi_put = MultiBufferPutRequest::new("multi-key", &multi_put_buffers)
        .tenant("tenant-a")
        .domain("domain-a")
        .object_set("set-a")
        .qos_tier("gold")
        .replication(policy.clone());
    assert_eq!(multi_put.tenant, Some("tenant-a"));
    assert_eq!(multi_put.domain, Some("domain-a"));
    assert_eq!(multi_put.object_set, Some("set-a"));
    assert_eq!(multi_put.qos_tier, Some("gold"));
    assert_eq!(multi_put.policy, Some(policy.clone()));

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

    // minor version difference should still be compatible
    let mut lease_c = lease_a.clone();
    lease_c.compatibility.store_api_minor_version += 3;
    assert!(compatibility_matches(&lease_a, &lease_c));
    assert!(compatibility_matches(&lease_c, &lease_a));

    // major version difference should be incompatible
    let mut lease_d = lease_a.clone();
    lease_d.compatibility.store_api_version += 1;
    assert!(!compatibility_matches(&lease_a, &lease_d));

    assert_eq!(control_bind_host(""), "127.0.0.1");
    assert_eq!(control_bind_host("0.0.0.0:7001"), "127.0.0.1");
    assert_eq!(control_bind_host("10.0.0.9:7001"), "10.0.0.9");
    assert_eq!(align_up_u64(17, 8), 24);
    assert_eq!(align_up_u64(64, 64), 64);
    assert!(now_ms() > 0);

    {
        let _guard = metrics_test_lock().lock();
        reset_metrics();
        record_success_metric("helper_metric", 12, 34);
        let metrics = render_prometheus_metrics();
        assert!(metrics.contains("operation=\"helper_metric\",status=\"ok\""));
        assert!(metrics.contains("mooncake_store_operation_bytes_in_total"));
    }

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
        .namespace_quota(NamespaceQuota::new().max_bytes(1024).max_objects(8))
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
    bootstrap_route_policy(
        metadata.as_ref(),
        &writer,
        "default",
        RouteControlMode::EmbeddedWrh,
        3,
    )
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
        "default",
        RouteControlMode::EmbeddedWrh,
        3,
    )
    .expect("matching route policy should pass");
    let error = bootstrap_route_policy(
        metadata.as_ref(),
        &follower,
        "default",
        RouteControlMode::EmbeddedWrh,
        4,
    )
    .expect_err("mismatched route_topk should fail");
    assert!(matches!(error, StoreError::InvalidState(_)));
}

#[test]
fn builder_auto_allocates_monotonic_epochs_for_same_stable_id() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let first = StoreClientBuilder::new(metadata.clone(), "auto-alloc")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7101")
        .segment_name("auto-alloc-segment-a")
        .build(test_future_expiry_ms())
        .expect("first auto-allocated runtime should build");
    let second = StoreClientBuilder::new(metadata, "auto-alloc")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7102")
        .segment_name("auto-alloc-segment-b")
        .build(test_future_expiry_ms())
        .expect("second auto-allocated runtime should build");

    assert_ne!(first.lease().runtime.epoch, second.lease().runtime.epoch);
    assert!(second.lease().runtime.epoch > first.lease().runtime.epoch);
}

#[test]
fn builder_allows_takeover_of_unreachable_duplicate_runtime() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut endpoints = ClientEndpointSet {
        rpc_address: "127.0.0.1:7103".to_string(),
        segment_name: Some(SegmentName::new("takeover-segment")),
        labels: BTreeMap::new(),
    };
    endpoints.labels.insert(
        control_address_label().to_string(),
        "127.0.0.1:1".to_string(),
    );
    metadata
        .upsert_client_lease(&ClientLease {
            runtime: ClientRuntimeId::new("takeover-runtime", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints,
            expires_at_ms: test_future_expiry_ms(),
        })
        .expect("stale runtime should publish");

    StoreClientBuilder::new(metadata, "takeover-runtime")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7104")
        .segment_name("takeover-segment")
        .build(test_future_expiry_ms())
        .expect("unreachable duplicate runtime should allow takeover");
}

#[test]
fn builder_auto_allocates_higher_epoch_when_newer_runtime_is_reachable() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let newer = StoreClientBuilder::new(metadata.clone(), "epoch-fence")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7105")
        .segment_name("epoch-fence-newer")
        .build(test_future_expiry_ms())
        .expect("first runtime should build");

    let successor = StoreClientBuilder::new(metadata, "epoch-fence")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7106")
        .segment_name("epoch-fence-older")
        .build(test_future_expiry_ms())
        .expect("successor runtime should auto-allocate a newer epoch");

    assert!(successor.lease().runtime.epoch > newer.lease().runtime.epoch);
}

#[test]
fn builder_rejects_live_segment_name_reuse_across_runtimes() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let _owner = StoreClientBuilder::new(metadata.clone(), "segment-owner-a")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7107")
        .segment_name("shared-startup-segment")
        .build(test_future_expiry_ms())
        .expect("segment owner should build");

    let error = match StoreClientBuilder::new(metadata, "segment-owner-b")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7108")
        .segment_name("shared-startup-segment")
        .build(test_future_expiry_ms())
    {
        Ok(_) => panic!("live segment name reuse should be rejected"),
        Err(error) => error,
    };

    assert!(matches!(error, StoreError::Conflict(_)));
}

#[test]
fn builder_ignores_stale_segment_owner_index_without_live_lease() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let owner = StoreClientBuilder::new(inner.clone(), "stale-owner")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7110")
        .segment_name("stale-shared-segment")
        .build(test_future_expiry_ms())
        .expect("segment owner should build");
    inner
        .update_client_state(owner.runtime_id(), ClientLifecycleState::Draining)
        .expect("owner should be marked non-active");

    let metadata = inner;
    let _replacement = StoreClientBuilder::new(metadata, "replacement-owner")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7111")
        .segment_name("stale-shared-segment")
        .build(test_future_expiry_ms())
        .expect("stale owner index without live lease should not block startup");
}

#[test]
fn builder_can_stage_active_until_local_memory_registration() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("staged-active-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "staged-active")
        .state(ClientLifecycleState::Active)
        .activate_on_local_memory_registration()
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
        .expect("staged client should build");

    assert_eq!(client.lease().state, ClientLifecycleState::Standby);
    let before = metadata
        .get_client_lease(client.runtime_id())
        .expect("staged lease lookup should succeed")
        .expect("staged lease should exist");
    assert_eq!(before.state, ClientLifecycleState::Standby);

    client
        .register_local_memory()
        .expect("local memory registration should activate staged client");

    assert_eq!(client.lease().state, ClientLifecycleState::Active);
    let after = metadata
        .list_live_clients()
        .expect("live clients should list")
        .into_iter()
        .find(|lease| lease.runtime == *client.runtime_id())
        .expect("activated lease should exist");
    assert_eq!(after.state, ClientLifecycleState::Active);
}

#[test]
fn staged_activation_republishes_lease_without_state_patch() {
    let metadata = Arc::new(CountingMetadataBackend::with_reject_state_patch(
        Arc::new(InMemoryMetadataBackend::new()),
        true,
    ));
    let transport = Arc::new(TestTransport::new("staged-active-refresh-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "staged-active-refresh")
        .state(ClientLifecycleState::Active)
        .activate_on_local_memory_registration()
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
        .expect("staged client should build");

    let upserts_before = metadata.upsert_client_lease_calls();
    client
        .register_local_memory()
        .expect("local memory registration should republish the active lease");

    assert_eq!(client.lease().state, ClientLifecycleState::Active);
    assert_eq!(metadata.update_client_state_calls(), 0);
    assert!(metadata.upsert_client_lease_calls() > upserts_before);
}

#[test]
fn heartbeat_keeps_staged_client_active_after_local_memory_registration() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("staged-heartbeat-segment"));
    let mut client = StoreClientBuilder::new(metadata.clone(), "staged-heartbeat")
        .state(ClientLifecycleState::Active)
        .activate_on_local_memory_registration()
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
        .expect("staged heartbeat client should build");

    client
        .register_local_memory()
        .expect("local memory registration should activate staged heartbeat client");
    client
        .heartbeat(test_future_expiry_ms())
        .expect("heartbeat should preserve the active lifecycle state");

    let lease = metadata
        .list_live_clients()
        .expect("live clients should list")
        .into_iter()
        .find(|lease| lease.runtime == *client.runtime_id())
        .expect("heartbeat lease should exist");
    assert_eq!(lease.state, ClientLifecycleState::Active);
}

#[test]
fn activate_republishes_lease_without_state_patch() {
    let metadata = Arc::new(CountingMetadataBackend::with_reject_state_patch(
        Arc::new(InMemoryMetadataBackend::new()),
        true,
    ));
    let transport = Arc::new(TestTransport::new("activate-refresh-segment"));
    let mut client = StoreClientBuilder::new(metadata.clone(), "activate-refresh")
        .state(ClientLifecycleState::Standby)
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
        .expect("standby client should build");

    let upserts_before = metadata.upsert_client_lease_calls();
    client
        .activate()
        .expect("activate should republish the lease instead of patching state");

    assert_eq!(client.lease().state, ClientLifecycleState::Active);
    assert_eq!(metadata.update_client_state_calls(), 0);
    assert!(metadata.upsert_client_lease_calls() > upserts_before);
}

#[test]
fn heartbeat_recovery_republishes_local_metadata_after_long_redis_outage() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let metadata = Arc::new(RecoverableMetadataBackend::new(inner));
    let transport = Arc::new(TestTransport::new("redis-recovery-segment"));
    let mut client = match StoreClientBuilder::new(metadata.clone(), "redis-recovery")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7012")
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
    {
        Ok(client) => client,
        Err(StoreError::Transport(message)) if message.contains("Operation not permitted") => {
            return;
        }
        Err(error) => panic!("client build should succeed: {error}"),
    };

    client
        .register_local_memory()
        .expect("local memory should register");
    let runtime = client.runtime_id().clone();
    let segment = client.segment_name().expect("segment should exist");
    assert_eq!(
        metadata
            .list_segments(Some(&runtime))
            .expect("segment should be visible before outage")
            .len(),
        1
    );

    metadata.hide_runtime_metadata(&runtime);
    metadata.hide_route_policy();
    metadata.fail_next_lease_upserts(2);
    assert!(client.heartbeat(now_ms().saturating_add(30_000)).is_err());
    assert!(client.heartbeat(now_ms().saturating_add(30_000)).is_err());
    assert!(metadata
        .list_live_clients()
        .expect("live clients should list during outage")
        .is_empty());
    assert!(metadata
        .list_segments(Some(&runtime))
        .expect("segments should list during outage")
        .is_empty());

    let republish_calls_before_repair = transport.republish_local_metadata_calls();
    client
        .heartbeat(now_ms().saturating_add(30_000))
        .expect("heartbeat should repair local metadata after redis recovery");
    assert!(
        transport.republish_local_metadata_calls() > republish_calls_before_repair,
        "heartbeat recovery should republish local transport metadata"
    );
    let live = metadata
        .list_live_clients()
        .expect("live clients should list after repair");
    assert!(live.iter().any(|lease| lease.runtime == runtime));
    assert_eq!(
        metadata
            .list_segments(Some(&runtime))
            .expect("segments should list after repair")
            .into_iter()
            .map(|announcement| announcement.segment_name)
            .collect::<Vec<_>>(),
        vec![segment]
    );
    assert!(metadata
        .get_route_policy(&RoutePolicyDomain::Default)
        .expect("route policy should read after repair")
        .is_some());
}

#[test]
fn route_topk_from_tenant_spec_extracts_topk() {
    let spec_with_topk = TenantPolicySpec {
        routing: Some(TenantRoutePolicy {
            route_topk: Some(4),
        }),
        ..TenantPolicySpec::default()
    };
    assert_eq!(route_topk_from_tenant_spec(&spec_with_topk), Some(4));

    let spec_without_topk = TenantPolicySpec {
        routing: Some(TenantRoutePolicy { route_topk: None }),
        ..TenantPolicySpec::default()
    };
    assert_eq!(route_topk_from_tenant_spec(&spec_without_topk), None);

    let spec_no_routing = TenantPolicySpec::default();
    assert_eq!(route_topk_from_tenant_spec(&spec_no_routing), None);
}

#[test]
fn bootstrap_route_policy_falls_back_to_default_when_tenant_override_missing() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer = ClientLease {
        runtime: ClientRuntimeId::new("writer", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: test_future_expiry_ms(),
    };

    bootstrap_route_policy(
        metadata.as_ref(),
        &writer,
        "tenant-a",
        RouteControlMode::EmbeddedWrh,
        3,
    )
    .expect("default bootstrap should succeed without tenant override");

    let effective = effective_route_policy(metadata.as_ref(), "tenant-a")
        .expect("effective route policy should fall back to default")
        .expect("effective policy should exist");
    assert_eq!(effective.route_control, RouteControlMode::EmbeddedWrh);
    assert_eq!(effective.route_topk, 3);
}

#[test]
fn builder_resolves_scoped_tenant_policy_without_listing_all_policies() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    inner
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    routing: Some(TenantRoutePolicy { route_topk: None }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("root tenant policy should be stored");
    inner
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", Some("default"), Some("default")),
                spec: TenantPolicySpec {
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(4),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 20,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("object-set tenant policy should be stored");

    let metadata = Arc::new(NoHotPathMetadataBackend::with_tenant_policy_list_blocked(
        inner.clone(),
    ));
    let client = StoreClientBuilder::new(metadata, "tenant-policy-scanless")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .route_topk(4)
        .transport(Arc::new(TestTransport::new(
            "tenant-policy-scanless-segment",
        )))
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("builder should not need tenant policy list scans");

    assert_eq!(client.route_control, RouteControlMode::MetadataOnly);
    assert_eq!(client.route_topk, 4);

    let effective = effective_route_policy(inner.as_ref(), "tenant-a")
        .expect("effective route policy should resolve from exact scope lookups")
        .expect("effective policy should exist");
    assert_eq!(effective.route_control, RouteControlMode::MetadataOnly);
    assert_eq!(effective.route_topk, 4);
}

#[test]
fn namespace_quota_evicts_within_same_tenant_before_rejecting() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    quota: Some(TenantQuotaPolicy {
                        max_bytes: Some(5),
                        max_objects: Some(1),
                    }),
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant quota policy should store");
    let transport = Arc::new(TestTransport::new("quota-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "quota-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    client
        .put("small", b"1234")
        .expect("first put should fit quota");
    client
        .put("big", b"12345")
        .expect("second put should evict the older object in the same tenant");

    assert!(client
        .query_route_in_tenant("tenant-a", "small")
        .expect("small route lookup should succeed")
        .is_none());
    assert_eq!(
        client
            .get("big")
            .expect("big should remain readable after tenant-local eviction"),
        b"12345"
    );

    let metrics = snapshot_metrics();
    let tenant_local_evictions = metrics
        .tenant_local_eviction
        .iter()
        .find(|sample| sample.key.result == "ok")
        .map(|sample| sample.value)
        .unwrap_or(0);
    let quota_ok = metrics
        .tenant_quota_reservation
        .iter()
        .find(|sample| sample.key.result == "ok")
        .map(|sample| sample.value)
        .unwrap_or(0);
    assert!(
        tenant_local_evictions >= 1,
        "quota exhaustion path should record at least one successful tenant-local eviction"
    );
    assert!(
        quota_ok >= 2,
        "successful writes should still record successful quota reservations"
    );

    let quota_after_success = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load after successful eviction write")
        .expect("quota state should exist after successful eviction write");
    assert!(
        quota_after_success.used_bytes <= 5,
        "tenant usage must stay within byte quota after successful eviction write; used_bytes={}",
        quota_after_success.used_bytes
    );
    assert!(
        quota_after_success.used_objects <= 1,
        "tenant usage must stay within object quota after successful eviction write; used_objects={} ",
        quota_after_success.used_objects
    );
    assert_eq!(quota_after_success.pending_reserved_bytes, 0);
    assert_eq!(quota_after_success.pending_reserved_objects, 0);

    let objects_error = client
        .put("other", b"123456")
        .expect_err("oversized object should still be rejected when nothing can make room");
    assert!(matches!(
        objects_error,
        StoreError::Conflict(_) | StoreError::QuotaExceeded { .. } | StoreError::Metadata(_)
    ));
    assert!(objects_error
        .to_string()
        .contains("tenant quota bytes exceeded"));

    assert!(client
        .query_route_in_tenant("tenant-a", "big")
        .expect("big route lookup after oversized write should succeed")
        .is_some());
    assert!(client
        .query_route_in_tenant("tenant-a", "other")
        .expect("oversized route lookup should succeed")
        .is_none());

    let quota = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load")
        .expect("quota state should exist");
    assert!(
        quota.used_bytes <= 5,
        "tenant usage must not exceed byte quota after failed oversized write; used_bytes={}",
        quota.used_bytes
    );
    assert!(
        quota.used_objects <= 1,
        "tenant usage must not exceed object quota after failed oversized write; used_objects={}",
        quota.used_objects
    );
    assert_eq!(quota.used_bytes, 5);
    assert_eq!(quota.used_objects, 1);
    assert_eq!(quota.pending_reserved_bytes, 0);
    assert_eq!(quota.pending_reserved_objects, 0);

    let metrics_after_failure = snapshot_metrics();
    let tenant_local_eviction_attempts = metrics_after_failure
        .tenant_local_eviction
        .iter()
        .filter(|sample| sample.key.result == "ok" || sample.key.result == "miss")
        .map(|sample| sample.value)
        .sum::<u64>();
    assert!(
        tenant_local_eviction_attempts >= 1,
        "oversized follow-up write should record a tenant-local eviction attempt"
    );
}

#[test]
fn non_default_tenant_without_policy_inherits_namespace_quota() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("inherited-namespace-quota-segment"));
    let client = StoreClientBuilder::new(metadata, "inherited-namespace-quota-client")
        .tenant("default-tenant")
        .namespace_quota(NamespaceQuota::new().max_bytes(4).max_objects(1))
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    let error = client
        .put_in_tenant("tenant-b", "overflow", b"12345")
        .expect_err("non-default tenant should not bypass inherited namespace quota");
    assert!(matches!(error, StoreError::QuotaExceeded { .. }));
    assert!(client
        .query_route_in_tenant("tenant-b", "overflow")
        .expect("tenant-b route lookup should succeed")
        .is_none());
}

#[test]
fn routing_only_tenant_policy_still_inherits_namespace_quota() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    quota: None,
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("routing-only tenant policy should store");
    let transport = Arc::new(TestTransport::new("routing-only-policy-quota-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "routing-only-policy-quota-client")
        .tenant("default-tenant")
        .namespace_quota(NamespaceQuota::new().max_bytes(4).max_objects(1))
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    let error = client
        .put_in_tenant("tenant-a", "overflow", b"12345")
        .expect_err("routing-only tenant policy should not disable inherited quota");
    assert!(matches!(error, StoreError::QuotaExceeded { .. }));
    assert!(client
        .query_route_in_tenant("tenant-a", "overflow")
        .expect("tenant-a route lookup should succeed")
        .is_none());
}

#[test]
fn namespace_quota_overwrite_uses_committed_delta() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    quota: Some(TenantQuotaPolicy {
                        max_bytes: Some(5),
                        max_objects: Some(1),
                    }),
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant quota policy should store");
    let transport = Arc::new(TestTransport::new("overwrite-quota-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "overwrite-quota-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    client
        .put("key", b"1234")
        .expect("initial put should succeed");
    client
        .put("key", b"12345")
        .expect("same object should grow by one byte within quota");
    let error = client
        .put("key", b"123456")
        .expect_err("same object should fail once growth exceeds remaining quota");
    assert!(matches!(
        error,
        StoreError::Conflict(_) | StoreError::QuotaExceeded { .. } | StoreError::Metadata(_)
    ));
    assert!(error.to_string().contains("tenant quota bytes exceeded"));

    let quota = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load")
        .expect("quota state should exist");
    assert_eq!(quota.used_bytes, 5);
    assert_eq!(quota.used_objects, 1);

    let accounting = metadata
        .get_tenant_object_accounting(&ObjectKey::new("tenant-a::key"))
        .expect("object accounting should load")
        .expect("object accounting should exist");
    assert_eq!(accounting.committed_length, 5);
}

#[test]
fn remove_refunds_quota_when_delete_becomes_authoritative() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    quota: Some(TenantQuotaPolicy {
                        max_bytes: Some(8),
                        max_objects: Some(1),
                    }),
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant quota policy should store");
    let transport = Arc::new(TestTransport::new("remove-quota-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "remove-quota-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    client.put("key", b"12345678").expect("put should succeed");
    client.remove("key", true).expect("remove should succeed");
    client
        .put("other", b"12345678")
        .expect("quota should be refunded immediately on delete");

    let quota = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load")
        .expect("quota state should exist");
    assert_eq!(quota.used_bytes, 8);
    assert_eq!(quota.used_objects, 1);
    assert_eq!(quota.pending_reserved_bytes, 0);
    assert_eq!(quota.pending_reserved_objects, 0);
    assert!(metadata
        .get_tenant_object_accounting(&ObjectKey::new("tenant-a::key"))
        .expect("deleted accounting lookup should succeed")
        .is_none());
}

#[test]
fn routed_batch_put_rejects_over_quota_all_or_nothing() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("batch-quota-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    quota: Some(TenantQuotaPolicy {
                        max_bytes: Some(7),
                        max_objects: Some(2),
                    }),
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant quota policy should store");
    let client = StoreClientBuilder::new(metadata.clone(), "batch-quota-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(1024))
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("client memory should register");

    let error = client
        .batch_put(&[
            PutRequest::new("key-a", b"1234")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().replica_count(1)),
            PutRequest::new("key-b", b"1234")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().replica_count(1)),
        ])
        .expect_err("batch admission should fail atomically when combined bytes exceed quota");
    assert!(matches!(
        error,
        StoreError::Conflict(_) | StoreError::QuotaExceeded { .. } | StoreError::Metadata(_)
    ));

    assert!(client
        .query_route_in_tenant("tenant-a", "key-a")
        .expect("route lookup should succeed")
        .is_none());
    assert!(client
        .query_route_in_tenant("tenant-a", "key-b")
        .expect("route lookup should succeed")
        .is_none());

    let quota = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load")
        .expect("quota state should exist");
    assert_eq!(quota.used_bytes, 0);
    assert_eq!(quota.used_objects, 0);
    assert_eq!(quota.pending_reserved_bytes, 0);
    assert_eq!(quota.pending_reserved_objects, 0);
}

#[test]
fn tenant_local_quota_eviction_does_not_touch_other_tenants() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    for tenant in ["tenant-a", "tenant-b"] {
        metadata
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new(tenant, None::<String>, None::<String>),
                    spec: TenantPolicySpec {
                        quota: Some(TenantQuotaPolicy {
                            max_bytes: Some(5),
                            max_objects: Some(1),
                        }),
                        routing: Some(TenantRoutePolicy {
                            route_topk: Some(2),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 10,
                    updated_by: "admin".to_string(),
                },
                None,
            )
            .expect("tenant quota policy should store");
    }
    let transport = Arc::new(TestTransport::new("tenant-local-quota-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "tenant-local-quota-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    client
        .put_in_tenant("tenant-b", "stable", b"BBBB")
        .expect("tenant-b seed write should succeed");
    client
        .put("old", b"AAAA")
        .expect("tenant-a seed write should succeed");
    client
        .put("new", b"AAAAA")
        .expect("tenant-a replacement write should succeed by evicting tenant-a only");

    assert!(client
        .query_route_in_tenant("tenant-a", "old")
        .expect("tenant-a old route lookup should succeed")
        .is_none());
    assert_eq!(
        client
            .get("new")
            .expect("tenant-a new value should remain readable after eviction"),
        b"AAAAA"
    );
    assert!(client
        .query_route_in_tenant("tenant-b", "stable")
        .expect("tenant-b route lookup should succeed")
        .is_some());

    let metrics = snapshot_metrics();
    let tenant_local_evictions = metrics
        .tenant_local_eviction
        .iter()
        .find(|sample| sample.key.result == "ok")
        .map(|sample| sample.value)
        .unwrap_or(0);
    assert!(
        tenant_local_evictions >= 1,
        "tenant-a quota pressure should record at least one successful tenant-local eviction"
    );

    let quota_a = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("tenant-a quota state should load")
        .expect("tenant-a quota state should exist");
    let quota_b = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-b",
            None::<String>,
            None::<String>,
        ))
        .expect("tenant-b quota state should load")
        .expect("tenant-b quota state should exist");
    assert_eq!(quota_a.used_bytes, 5);
    assert_eq!(quota_a.used_objects, 1);
    assert!(
        quota_a.used_bytes <= 5,
        "tenant-a usage must stay within byte quota after successful eviction write; used_bytes={}",
        quota_a.used_bytes
    );
    assert!(
        quota_a.used_objects <= 1,
        "tenant-a usage must stay within object quota after successful eviction write; used_objects={}",
        quota_a.used_objects
    );
    assert_eq!(quota_b.used_bytes, 4);
    assert_eq!(quota_b.used_objects, 1);
    assert!(
        quota_b.used_bytes <= 5,
        "tenant-b usage must stay within byte quota; used_bytes={}",
        quota_b.used_bytes
    );
    assert!(
        quota_b.used_objects <= 1,
        "tenant-b usage must stay within object quota; used_objects={}",
        quota_b.used_objects
    );

    let tenant_b_value = client
        .get_in_tenant("tenant-b", "stable")
        .expect("tenant-b value should remain readable");
    assert_eq!(tenant_b_value, b"BBBB");
}

#[test]
fn tenant_local_quota_eviction_preserves_other_tenant_same_logical_key() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    for tenant in ["tenant-a", "tenant-b"] {
        metadata
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new(tenant, None::<String>, None::<String>),
                    spec: TenantPolicySpec {
                        quota: Some(TenantQuotaPolicy {
                            max_bytes: Some(5),
                            max_objects: Some(1),
                        }),
                        routing: Some(TenantRoutePolicy {
                            route_topk: Some(2),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 10,
                    updated_by: "admin".to_string(),
                },
                None,
            )
            .expect("tenant quota policy should store");
    }
    let transport = Arc::new(TestTransport::new("tenant-local-quota-same-key-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "tenant-local-quota-same-key-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    client
        .put_in_tenant("tenant-b", "shared-key", b"BBBB")
        .expect("tenant-b shared-key seed write should succeed");
    client
        .put("old", b"AAAA")
        .expect("tenant-a seed write should succeed");
    client
        .put("shared-key", b"AAAAA")
        .expect("tenant-a replacement write should succeed by evicting tenant-a only");

    assert!(client
        .query_route_in_tenant("tenant-a", "old")
        .expect("tenant-a old route lookup should succeed")
        .is_none());
    assert!(client
        .query_route_in_tenant("tenant-a", "shared-key")
        .expect("tenant-a shared-key route lookup should succeed")
        .is_some());
    assert!(client
        .query_route_in_tenant("tenant-b", "shared-key")
        .expect("tenant-b shared-key route lookup should succeed")
        .is_some());
    assert_eq!(
        client
            .get("shared-key")
            .expect("tenant-a shared-key should remain readable after eviction"),
        b"AAAAA"
    );
    assert_eq!(
        client
            .get_in_tenant("tenant-b", "shared-key")
            .expect("tenant-b shared-key should remain readable after tenant-a eviction"),
        b"BBBB"
    );

    let quota_a = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("tenant-a quota state should load")
        .expect("tenant-a quota state should exist");
    let quota_b = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-b",
            None::<String>,
            None::<String>,
        ))
        .expect("tenant-b quota state should load")
        .expect("tenant-b quota state should exist");
    assert_eq!(quota_a.used_bytes, 5);
    assert_eq!(quota_a.used_objects, 1);
    assert_eq!(quota_b.used_bytes, 4);
    assert_eq!(quota_b.used_objects, 1);
}

#[test]
fn routed_batch_put_preserves_cross_tenant_quota_isolation() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("batch-cross-tenant-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    for tenant in ["tenant-a", "tenant-b"] {
        metadata
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new(tenant, None::<String>, None::<String>),
                    spec: TenantPolicySpec {
                        quota: Some(TenantQuotaPolicy {
                            max_bytes: Some(4),
                            max_objects: Some(1),
                        }),
                        routing: Some(TenantRoutePolicy {
                            route_topk: Some(2),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 10,
                    updated_by: "admin".to_string(),
                },
                None,
            )
            .expect("tenant quota policy should store");
    }
    let client = StoreClientBuilder::new(metadata.clone(), "batch-cross-tenant-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(1024))
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("client memory should register");

    let routes = client
        .batch_put(&[
            PutRequest::new("key-a", b"aaaa")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().replica_count(1)),
            PutRequest::new("key-b", b"bbbb")
                .tenant("tenant-b")
                .replication(ReplicationPolicy::new().replica_count(1)),
        ])
        .expect("cross-tenant batch should succeed when each tenant stays within quota");
    assert_eq!(routes.len(), 2);

    let quota_a = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("tenant-a quota state should load")
        .expect("tenant-a quota state should exist");
    let quota_b = metadata
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-b",
            None::<String>,
            None::<String>,
        ))
        .expect("tenant-b quota state should load")
        .expect("tenant-b quota state should exist");
    assert_eq!(quota_a.used_bytes, 4);
    assert_eq!(quota_a.used_objects, 1);
    assert_eq!(quota_b.used_bytes, 4);
    assert_eq!(quota_b.used_objects, 1);
}

#[test]
fn put_returns_error_when_route_publish_succeeds_but_quota_finalize_fails() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    inner
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    quota: Some(TenantQuotaPolicy {
                        max_bytes: Some(16),
                        max_objects: Some(2),
                    }),
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant quota policy should store");
    let metadata = Arc::new(FinalizeFailureMetadataBackend::new(
        inner.clone(),
        "quota-client:1:tenant-a:1",
    ));
    let transport = Arc::new(TestTransport::new("finalize-failure-segment"));
    let client = StoreClientBuilder::new(metadata, "quota-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("quota client should build");

    let error = client
        .put("key", b"1234")
        .expect_err("put should fail when quota finalization fails");
    assert!(matches!(error, StoreError::InvalidState(_)));
    assert!(error.to_string().contains("injected finalize failure"));

    let route = client
        .query_route_in_tenant("tenant-a", "key")
        .expect("route lookup should succeed")
        .expect("route should have been published before finalize failure");
    assert_eq!(route.replicas[0].length, 4);

    let quota = inner
        .get_tenant_quota_state(&TenantPolicyScope::new(
            "tenant-a",
            None::<String>,
            None::<String>,
        ))
        .expect("quota state should load")
        .expect("quota state should exist");
    assert_eq!(quota.used_bytes, 0);
    assert_eq!(quota.used_objects, 0);
    assert_eq!(quota.pending_reserved_bytes, 4);
    assert_eq!(quota.pending_reserved_objects, 1);
}

#[test]
fn runtime_uses_metadata_authored_fairness_shaping_and_placement_defaults() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("runtime-policy-client-segment"));
    let owner_a = publish_storage_node(&metadata, &transport, "owner-a", "seg-a", "pool-a");
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    routing: Some(TenantRoutePolicy {
                        route_topk: Some(2),
                    }),
                    fairness: Some(TenantExecutionFairnessPolicy {
                        max_remote_batch_items_per_tenant: Some(3),
                    }),
                    shaping: Some(TenantBandwidthShapingPolicy {
                        max_remote_batch_bytes: Some(64),
                        max_remote_batch_burst_items: Some(5),
                        max_inflight_bytes_per_batch: Some(17),
                    }),
                    placement: Some(TenantPlacementPolicy {
                        default_replica_count: Some(2),
                        prefer_local: Some(false),
                        prefer_alloc_in_same_node: Some(true),
                        preferred_storage_owners: Some(vec!["owner-a".to_string()]),
                        preferred_segments: Some(vec!["seg-a".to_string()]),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant runtime policy should store");
    let client = StoreClientBuilder::new(metadata, "runtime-policy-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("runtime policy client should build");

    wait_for_runtime_visibility(&client, &owner_a);

    assert_eq!(client.fairness_max_remote_batch_items_per_tenant(), Some(3));
    assert_eq!(client.shaping_max_remote_batch_bytes(), Some(64));
    assert_eq!(client.shaping_max_inflight_bytes_per_batch(), Some(17));
    assert_eq!(client.default_replica_count(), 2);

    let resolved = client
        .resolve_replication_policy(None)
        .expect("default replication policy should resolve");
    assert_eq!(resolved.replica_count, 2);
    assert!(resolved.required_preferred_segments.is_empty());
    assert!(resolved
        .hint_preferred_segments
        .contains(&SegmentName::new("seg-a")));
    assert_eq!(resolved.preferred_storage_runtimes, vec![owner_a]);
    assert!(resolved.prefer_local);
}

#[test]
fn tenant_policy_preferred_segments_missing_fall_back_for_put_and_batch_put() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("tenant-policy-fallback-writer-segment"));
    let owner = publish_storage_node(
        &metadata,
        &transport,
        "fallback-owner",
        "seg-live",
        "pool-a",
    );
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    placement: Some(TenantPlacementPolicy {
                        preferred_segments: Some(vec!["seg-missing".to_string()]),
                        ..TenantPlacementPolicy::default()
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant policy should store");
    let client = StoreClientBuilder::new(metadata.clone(), "tenant-policy-fallback-writer")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer should build");
    client
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_runtime_visibility(&client, &owner);

    let single = client
        .put("single-fallback", b"alpha")
        .expect("tenant-policy hint should fall back for put");
    assert_eq!(
        single.replicas[0].segment_name,
        SegmentName::new("seg-live")
    );

    let batch = client
        .batch_put(&[PutRequest::new("batch-fallback", b"bravo")])
        .expect("tenant-policy hint should fall back for batch_put");
    assert_eq!(
        batch[0].replicas[0].segment_name,
        SegmentName::new("seg-live")
    );

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains(
        "mooncake_store_preferred_segment_skip_total{tenant=\"tenant-a\",source=\"tenant_policy\",reason=\"not_found\"} 2"
    ));
}

#[test]
fn request_preferred_segments_preserve_hard_and_soft_pin_semantics() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("request-preferred-segment-writer"));
    let owner = publish_storage_node(&metadata, &transport, "request-owner", "seg-live", "pool-a");
    let client = StoreClientBuilder::new(metadata.clone(), "request-preferred-segment-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer should build");
    client
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_runtime_visibility(&client, &owner);

    let hard_error = client
        .put_with_policy(
            "hard-missing",
            b"alpha",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_segment("seg-missing"),
        )
        .expect_err("hard preferred segment should still fail");
    assert!(matches!(hard_error, StoreError::NotFound(_)));

    let soft_route = client
        .put_with_policy(
            "soft-missing",
            b"bravo",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .with_soft_pin(true)
                .preferred_segment("seg-missing"),
        )
        .expect("soft preferred segment should fall back");
    assert_eq!(
        soft_route.replicas[0].segment_name,
        SegmentName::new("seg-live")
    );
}

#[test]
fn tenant_policy_preferred_segments_still_prefer_live_segment() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("tenant-policy-preferred-live-writer"));
    let owner_a = publish_storage_node(&metadata, &transport, "prefer-owner-a", "seg-a", "pool-a");
    let _owner_b = publish_storage_node(&metadata, &transport, "prefer-owner-b", "seg-b", "pool-a");
    metadata
        .put_tenant_policy(
            &TenantPolicy {
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                spec: TenantPolicySpec {
                    placement: Some(TenantPlacementPolicy {
                        preferred_segments: Some(vec!["seg-a".to_string()]),
                        ..TenantPlacementPolicy::default()
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("tenant policy should store");
    let client = StoreClientBuilder::new(metadata.clone(), "tenant-policy-preferred-live-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer should build");
    client
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_runtime_visibility(&client, &owner_a);

    let route = client
        .put("preferred-live", b"payload")
        .expect("live tenant-policy preferred segment should be used");
    assert_eq!(route.replicas[0].segment_name, SegmentName::new("seg-a"));
}

#[test]
fn compatibility_facade_surface_covers_aliases_and_buffers() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("facade-segment"));
    let mut client = StoreClientBuilder::new(metadata.clone(), "facade-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .route_control(RouteControlMode::MetadataOnly)
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
        tenant_plain.namespace,
        Some(NamespaceScope::with_defaults(Some("tenant-b"), None, None))
    );
    assert_eq!(tenant_plain.logical_key.as_deref(), Some("plain"));
    assert_eq!(
        tenant_plain.canonical_key.as_deref(),
        Some("tenant-b/default/default/plain")
    );
    assert_eq!(tenant_plain.sharing_scope.as_deref(), Some("tenant-b"));
    assert_eq!(tenant_plain.qos_tier.as_deref(), Some("default"));
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
    assert_eq!(queried_tenant.namespace, tenant_plain.namespace);
    assert_eq!(queried_tenant.logical_key, tenant_plain.logical_key);
    assert_eq!(queried_tenant.canonical_key, tenant_plain.canonical_key);
    assert_eq!(queried_tenant.sharing_scope, tenant_plain.sharing_scope);
    assert_eq!(queried_tenant.qos_tier, tenant_plain.qos_tier);

    let queried_scope = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("tenant-b"), None, None),
            "plain",
        )
        .expect("scope query should succeed")
        .expect("scope route should exist");
    assert_eq!(queried_scope.key, queried_tenant.key);
    let queried_object = client
        .query_route_by_object_id(&LogicalObjectId::new(
            NamespaceScope::with_defaults(Some("tenant-b"), None, None),
            "plain",
        ))
        .expect("object query should succeed")
        .expect("object route should exist");
    assert_eq!(queried_object.key, queried_tenant.key);
    let tenant_b_scope = NamespaceScope::with_defaults(Some("tenant-b"), None, None);
    let scoped_routes = client
        .list_routes_in_scope(&tenant_b_scope)
        .expect("scope listing should succeed");
    assert!(scoped_routes
        .iter()
        .any(|route| route.key == queried_tenant.key));
    assert!(scoped_routes
        .iter()
        .all(|route| route.namespace.as_ref() == Some(&tenant_b_scope)));

    let tenant_b_reuse = mooncake_store_core::ReuseIdentity::new(
        "tenant-b",
        "default",
        "tenant-b",
        "tenant-b/default/default/plain",
    );
    let reuse_candidates = client
        .list_reuse_candidates(&tenant_b_reuse)
        .expect("reuse lookup should succeed");
    assert!(reuse_candidates
        .iter()
        .any(|route| route.key == queried_tenant.key));
    assert!(reuse_candidates
        .iter()
        .all(|route| route.namespace.as_ref() == Some(&tenant_b_scope)));

    let tenant_a_reuse = mooncake_store_core::ReuseIdentity::new(
        "tenant-a",
        "default",
        "tenant-a",
        "tenant-a/default/default/plain",
    );
    let tenant_a_candidates = client
        .list_reuse_candidates(&tenant_a_reuse)
        .expect("tenant-a reuse lookup should succeed");
    assert!(tenant_a_candidates
        .iter()
        .any(|route| route.key == queried.key));
    assert!(tenant_a_candidates
        .iter()
        .all(|route| route.namespace.as_ref()
            == Some(&NamespaceScope::with_defaults(Some("tenant-a"), None, None))));
    assert!(tenant_a_candidates
        .iter()
        .all(|route| route.key != queried_tenant.key));

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
    let mut client = StoreClientBuilder::new(metadata.clone(), "adapter-client")
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

    let local_authority = LocalAuthorityAdapter {
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        route_write_gate: test_route_write_gate(),
    };
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
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
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

    let draining_authority = LocalAuthorityAdapter {
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Draining),
        route_write_gate: test_route_write_gate(),
    };
    assert_eq!(
        draining_authority
            .get_route(&namespace, &authority, &scoped_key)
            .expect("draining authority should still serve reads"),
        Some(route.clone())
    );
    assert!(matches!(
        draining_authority.compare_and_swap_route(
            &namespace,
            &authority,
            &scoped_key,
            Some(route.version),
            Some(&updated),
        ),
        Err(StoreError::InvalidState(_))
    ));
    let draining_replace = draining_authority.batch_replace_routes(
        &namespace,
        &authority,
        &[RouteCasRequest {
            key: scoped_key.clone(),
            expected: None,
            next: Some(route.clone()),
        }],
    );
    assert!(matches!(
        draining_replace[0],
        Err(StoreError::InvalidState(_))
    ));

    let draining_allocator = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: client.storage_owner.clone(),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Draining),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
    };
    assert!(matches!(
        draining_allocator.reserve_any(client.runtime_id(), 8),
        Err(StoreError::InvalidState(_))
    ));
    assert!(draining_allocator
        .batch_reserve_any(client.runtime_id(), &[8, 8])
        .into_iter()
        .all(|result| matches!(result, Err(StoreError::InvalidState(_)))));

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
    client
        .enter_draining()
        .expect("client should enter draining");
    let draining_rpc_get = control
        .batch_get_routes(
            &client.lease(),
            &namespace,
            &authority,
            std::slice::from_ref(&scoped_key),
        )
        .expect("draining rpc get should still reply");
    assert!(draining_rpc_get[0]
        .as_ref()
        .expect("draining route get should decode")
        .is_some());
    let draining_rpc_cas = control
        .batch_compare_and_swap_routes(
            &client.lease(),
            &namespace,
            &authority,
            &[RouteCasRequest {
                key: scoped_key.clone(),
                expected: None,
                next: Some(route.clone()),
            }],
        )
        .expect("draining rpc cas should still reply");
    assert!(matches!(
        draining_rpc_cas[0],
        Err(StoreError::InvalidState(_))
    ));
    let draining_rpc_reserve = control
        .batch_reserve_any(&client.lease(), client.runtime_id(), &[8])
        .expect("draining reserve should still reply");
    assert!(matches!(
        draining_rpc_reserve[0],
        Err(StoreError::InvalidState(_))
    ));
    let missing_rpc_get = control
        .batch_get_routes(
            &client.lease(),
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

    let reclaim_reservation = client
        .reserve_segment_allocation(client.runtime_id(), None, 16, true)
        .expect("reclaim reservation should succeed");
    {
        let mut state = client.state.lock();
        state.pending_reclaims.push_back(PendingReclaim {
            due_at_ms: 0,
            policy_rank: 1,
            tenant: "tenant-a".to_string(),
            qos_tier: "default".to_string(),
            storage_runtime: ClientRuntimeId::new("stale-owner", ClientEpoch(1)),
            segment_name: SegmentName::new("stale-segment"),
            offset_bytes: 0,
            length_bytes: 16,
        });
        state.pending_reclaims.push_back(PendingReclaim {
            due_at_ms: 0,
            policy_rank: 1,
            tenant: "tenant-a".to_string(),
            qos_tier: "default".to_string(),
            storage_runtime: client.runtime_id().clone(),
            segment_name: reclaim_reservation.segment_name.clone(),
            offset_bytes: reclaim_reservation.offset_bytes,
            length_bytes: reclaim_reservation.length_bytes,
        });
    }
    assert_eq!(client.allocator.lock().usage_bytes().0, 16);
    client
        .flush_due_reclaims()
        .expect("flush_due_reclaims should skip unavailable runtimes");
    assert_eq!(
        client.allocator.lock().usage_bytes().0,
        0,
        "local reclaim should still release reserved bytes"
    );
    assert!(
        client.state.lock().pending_reclaims.is_empty(),
        "due reclaim queue should be drained after flush"
    );

    let primary = client.segment_name().expect("primary segment should exist");
    let (_target, reservation) = client
        .reserve_specific_segment(client.runtime_id(), &primary, 8, true)
        .expect("reserve_specific_segment should succeed");
    client
        .release_reserved_allocations(
            &[ReplicaWriteTarget {
                storage_runtime: client.runtime_id().clone(),
                segment_name: primary.clone(),
                transport_endpoint: None,
                target_chunks: Vec::new(),
            }],
            std::slice::from_ref(&reservation),
        )
        .expect("release_reserved_allocations should succeed");
    assert!(matches!(
        client.release_reserved_allocations(
            &[ReplicaWriteTarget {
                storage_runtime: client.runtime_id().clone(),
                segment_name: primary,
                transport_endpoint: None,
                target_chunks: Vec::new(),
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
    let remaining_segments = store_a
        .list_segments()
        .expect("segment listing should succeed");
    assert!(
        remaining_segments.is_empty(),
        "all drained local segments should retire after shrink: {remaining_segments:?}"
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
fn remote_get_recomputes_segment_base_after_restart() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("restart-read-store-segment"));
    let reader_transport = Arc::new(store_transport.peer("restart-read-reader-segment"));

    let store = StoreClientBuilder::new(metadata.clone(), "restart-read-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let reader = StoreClientBuilder::new(metadata, "restart-read-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store, &reader]);

    let key = "restart-read-key";
    let value = b"restart-read-value";
    store.put(key, value).expect("seed put should succeed");
    assert_eq!(
        reader.get(key).expect("initial reader get should succeed"),
        value
    );

    let segment = store.segment_name().expect("store segment should exist");
    store_transport.restart_external_segment(&segment.0);

    assert_eq!(
        reader
            .get(key)
            .expect("reader get after restart should succeed"),
        value
    );
}

#[test]
fn remote_put_refreshes_cached_segment_handle_after_restart() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("restart-write-store-segment"));
    let router_transport = Arc::new(store_transport.peer("restart-write-router-segment"));
    let reader_transport = Arc::new(store_transport.peer("restart-write-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store = StoreClientBuilder::new(metadata.clone(), "restart-write-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "restart-write-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata, "restart-write-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store, &router, &reader]);

    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false)
        .preferred_storage_owners([store.runtime_id().storage_key()]);

    router
        .put_with_policy("restart-write-key-1", b"warmup", &policy)
        .expect("warmup put should succeed");
    assert_eq!(
        reader
            .get("restart-write-key-1")
            .expect("warmup reader get should succeed"),
        b"warmup"
    );

    let segment = store.segment_name().expect("store segment should exist");
    store_transport.restart_external_segment(&segment.0);

    router
        .put_with_policy("restart-write-key-2", b"after-restart", &policy)
        .expect("put after restart should succeed");

    assert_eq!(
        reader
            .get("restart-write-key-1")
            .expect("reader should still see preserved warmup data"),
        b"warmup"
    );
    assert_eq!(
        reader
            .get("restart-write-key-2")
            .expect("reader should see post-restart data"),
        b"after-restart"
    );
}

#[test]
fn local_read_prefers_replica_target_offset_over_segment_offset() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("local-target-offset-segment"));
    let store = StoreClientBuilder::new(metadata, "local-target-offset-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(256))
        .build(test_future_expiry_ms())
        .expect("store build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    let segment = store.segment_name().expect("store segment should exist");
    let key = "local-target-offset-key";
    let payload = b"target-offset-payload";
    let distractor = b"wrong-offset-payload!";
    assert_eq!(payload.len(), distractor.len());

    let base = {
        let state = store.state.lock();
        state
            .memory_ref()
            .expect("memory should exist")
            .storage_address(&segment, 0)
            .expect("segment base should resolve")
    };
    let target_offset = 64usize;
    unsafe {
        ptr::copy_nonoverlapping(distractor.as_ptr(), base.cast::<u8>(), distractor.len());
        ptr::copy_nonoverlapping(
            payload.as_ptr(),
            base.cast::<u8>().add(target_offset),
            payload.len(),
        );
    }

    let object_id = LogicalObjectId::new(NamespaceScope::default(), key);
    let mut route = ObjectRoute {
        key: ObjectKey::from_logical_id(&object_id),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: Some(mooncake_store_core::DEFAULT_QOS_TIER.to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: store.lease.compatibility.clone(),
        replicas: vec![ReplicaRoute {
            owner: store.runtime_id().clone(),
            segment_name: segment,
            offset: Some(base as u64 + target_offset as u64),
            segment_offset: 0,
            length: payload.len() as u64,
            checksum: Some(payload_checksum(payload)),
            tier: mooncake_store_core::ReplicaTier::Dram,
            priority: 0,
        }],
    };
    mooncake_store_core::apply_route_identity(&mut route, &object_id);
    store
        .route_directory
        .compare_and_swap_object_route(&store.lease, &route.key, None, Some(&route))
        .expect("route publish should succeed");

    assert_eq!(
        store.get(key).expect("local get should use target offset"),
        payload
    );
    assert_eq!(
        store
            .batch_get(&[ObjectRef::new(key)])
            .expect("local batch get should use target offset"),
        vec![payload.to_vec()]
    );
}

#[test]
fn stale_route_checksum_mismatch_refreshes_to_latest_route() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("overwrite-refresh-store-segment"));
    let reader_transport = Arc::new(store_transport.peer("overwrite-refresh-reader-segment"));

    let store = StoreClientBuilder::new(metadata.clone(), "overwrite-refresh-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport)
        .local_memory(storage_config_with_bytes(32))
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let reader = StoreClientBuilder::new(metadata, "overwrite-refresh-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store, &reader]);

    let key = "overwrite-refresh-key";
    let route_v1 = store
        .put(key, b"aaaaaaaa")
        .expect("first overwrite put should succeed");
    let _route_v2 = store
        .put(key, b"bbbbbbbb")
        .expect("second overwrite put should succeed");
    let route_v3 = store
        .put(key, b"cccccccc")
        .expect("third overwrite put should succeed");
    assert_eq!(
        route_v3.replicas[0].segment_offset, route_v1.replicas[0].segment_offset,
        "third overwrite should reuse the reclaimed v1 slot"
    );

    let transport = reader.transport().expect("reader transport should exist");
    let mut resolved = resolved_object_for_route(&reader, "default", key, route_v1.clone());
    let mut buffer = vec![0u8; route_v1.replicas[0].length as usize];
    let mut checked_runtimes = BTreeSet::new();
    let local_segments = reader.local_storage_segments();
    let request_deadline = reader.request_deadline_for_transfer(buffer.len() as u64, 1);
    reader
        .read_single_object_with_failover(
            transport,
            &mut resolved,
            &mut buffer,
            &mut checked_runtimes,
            &local_segments,
            request_deadline,
        )
        .expect("stale checksum should refresh to the latest route");

    assert_eq!(buffer, b"cccccccc");
    assert_eq!(resolved.route.version, route_v3.version);
    assert_eq!(
        resolved.replica.segment_name,
        route_v3.replicas[0].segment_name
    );
    assert_eq!(
        resolved.replica.segment_offset,
        route_v3.replicas[0].segment_offset
    );
}

#[test]
fn stale_route_transport_failure_refreshes_to_republished_route() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("refresh-route-store-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("refresh-route-store-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("refresh-route-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("refresh-route-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "refresh-route-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "refresh-route-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "refresh-route-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "refresh-route-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
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

    let route_v1 = writer
        .put_with_policy(
            "refresh-route-key",
            b"route-a",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([store_a.runtime_id().storage_key()]),
        )
        .expect("initial route should land on store-a");
    assert_eq!(route_v1.replicas[0].owner, *store_a.runtime_id());

    let route_v2 = writer
        .put_with_policy(
            "refresh-route-key",
            b"route-b",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([store_b.runtime_id().storage_key()]),
        )
        .expect("overwrite route should land on store-b");
    assert_eq!(route_v2.replicas[0].owner, *store_b.runtime_id());

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("refresh-route-store-a-segment")
            .expect("store-a segment handle should exist");
        state
            .segments_by_handle
            .remove(&handle)
            .expect("store-a segment body should exist");
    }

    let transport = reader.transport().expect("reader transport should exist");
    let mut resolved =
        resolved_object_for_route(&reader, "default", "refresh-route-key", route_v1.clone());
    let mut buffer = vec![0u8; route_v1.replicas[0].length as usize];
    let mut checked_runtimes = BTreeSet::new();
    let local_segments = reader.local_storage_segments();
    let request_deadline = reader.request_deadline_for_transfer(buffer.len() as u64, 1);
    reader
        .read_single_object_with_failover(
            transport,
            &mut resolved,
            &mut buffer,
            &mut checked_runtimes,
            &local_segments,
            request_deadline,
        )
        .expect("stale transport failure should refresh to the republished route");

    assert_eq!(buffer, b"route-b");
    assert_eq!(resolved.route.version, route_v2.version);
    assert_eq!(resolved.replica.owner, *store_b.runtime_id());
}

#[test]
fn draining_route_authority_does_not_special_mirror_local_routes_before_restart() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("authority-drain-store-segment"));
    let authority_transport = Arc::new(store_transport.peer("authority-drain-victim-segment"));
    let writer_transport = Arc::new(store_transport.peer("authority-drain-writer-segment"));
    let reader_transport = Arc::new(store_transport.peer("authority-drain-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store = StoreClientBuilder::new(metadata.clone(), "authority-drain-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "authority-drain-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let mut authority = StoreClientBuilder::new(metadata.clone(), "authority-drain-victim")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "authority-drain-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(authority_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("authority build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "authority-drain-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route_scope", "authority-drain-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "authority-drain-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route_scope", "authority-drain-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    authority
        .register_local_memory()
        .expect("authority memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store, &authority, &writer, &reader]);

    let key = "authority-drain-key";
    let value = b"authority-drain-value";
    let route = writer
        .put_with_policy(
            key,
            value,
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([store.runtime_id().storage_key()]),
        )
        .expect("seed route should write to store");
    assert_eq!(route.replicas[0].owner, *store.runtime_id());

    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new(format!("default::{key}"));
    authority_replace(&namespace, &store.runtime_id().stable_id, &scoped_key, None)
        .expect("test should remove store mirror");
    authority_replace(
        &namespace,
        &authority.runtime_id().stable_id,
        &scoped_key,
        Some(&route),
    )
    .expect("test should leave route only on draining authority");
    // Simulate graceful shutdown with a stale membership snapshot.
    authority
        .live_client_cache
        .lock()
        .store(vec![authority.lease().clone()]);

    let migrated = authority
        .evacuate_owned_replicas()
        .expect("authority drain should not special-mirror local routes");
    assert_eq!(migrated, 0);
    authority_replace(
        &namespace,
        &authority.runtime_id().stable_id,
        &scoped_key,
        None,
    )
    .expect("test should simulate authority process restart");

    assert!(matches!(reader.get(key), Err(StoreError::NotFound(_))));
}

#[test]
fn drain_migration_updates_object_route_without_refreshing_draining_authority() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let victim_transport = Arc::new(TestTransport::new("drain-sync-victim-segment"));
    let survivor_transport = Arc::new(victim_transport.peer("drain-sync-survivor-segment"));
    let writer_transport = Arc::new(victim_transport.peer("drain-sync-writer-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut victim = StoreClientBuilder::new(metadata.clone(), "drain-sync-victim")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "true")
        .label("route_scope", "drain-sync-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(victim_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("victim build should succeed");
    let survivor = StoreClientBuilder::new(metadata.clone(), "drain-sync-survivor")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "true")
        .label("route_scope", "drain-sync-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(survivor_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("survivor build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "drain-sync-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "drain-sync-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    victim
        .register_local_memory()
        .expect("victim memory should register");
    survivor
        .register_local_memory()
        .expect("survivor memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_membership_convergence(&[&victim, &survivor, &writer]);

    let key = "drain-sync-key";
    let value = b"drain-sync-value";
    let route = writer
        .put_with_policy(
            key,
            value,
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([victim.runtime_id().storage_key()]),
        )
        .expect("seed route should write to victim");
    assert_eq!(route.replicas[0].owner, *victim.runtime_id());

    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new(format!("default::{key}"));
    authority_replace(
        &namespace,
        &victim.runtime_id().stable_id,
        &scoped_key,
        Some(&route),
    )
    .expect("test should leave a stale local authority route on victim");

    let migrated = victim
        .evacuate_owned_replicas()
        .expect("victim drain should migrate owned route");
    assert_eq!(migrated, 1);

    let refreshed = writer
        .query_route(key)
        .expect("object route lookup should succeed")
        .expect("migrated object route should exist");
    assert!(
        refreshed
            .replicas
            .iter()
            .all(|replica| replica.owner != *victim.runtime_id()),
        "object route facade must stop advertising the evacuated replica"
    );
    assert_eq!(writer.get(key).expect("migrated value should read"), value);
}

#[test]
fn readable_replica_selection_prefers_local_survivor() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let remote_transport = Arc::new(TestTransport::new("local-survivor-remote-segment"));
    let local_transport = Arc::new(remote_transport.peer("local-survivor-local-segment"));

    let remote = StoreClientBuilder::new(metadata.clone(), "local-survivor-remote")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "local-survivor-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(remote_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("remote build should succeed");
    let local = StoreClientBuilder::new(metadata, "local-survivor-local")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "local-survivor-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(local_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("local build should succeed");

    remote
        .register_local_memory()
        .expect("remote memory should register");
    local
        .register_local_memory()
        .expect("local memory should register");
    wait_for_membership_convergence(&[&remote, &local]);

    let remote_segment = remote.segment_name().expect("remote segment should exist");
    let local_segment = local.segment_name().expect("local segment should exist");
    let route = ObjectRoute {
        key: ObjectKey::new("default::local-survivor-key"),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: Some(mooncake_store_core::DEFAULT_QOS_TIER.to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: local.lease().compatibility,
        replicas: vec![
            ReplicaRoute {
                owner: remote.runtime_id().clone(),
                segment_name: remote_segment,
                offset: Some(0),
                segment_offset: 0,
                length: 8,
                checksum: None,
                tier: mooncake_store_core::ReplicaTier::Dram,
                priority: 0,
            },
            ReplicaRoute {
                owner: local.runtime_id().clone(),
                segment_name: local_segment.clone(),
                offset: Some(0),
                segment_offset: 0,
                length: 8,
                checksum: None,
                tier: mooncake_store_core::ReplicaTier::Dram,
                priority: 1,
            },
        ],
    };

    let local_segments = local.local_storage_segments();
    let readable = local
        .readable_runtime_set(true)
        .expect("readable runtimes should resolve");
    let selected = StoreClient::select_readable_replica(
        &route,
        local.runtime_id(),
        &local_segments,
        &readable,
    )
    .expect("route should expose a readable replica");
    assert_eq!(selected.segment_name, local_segment);
    assert_eq!(selected.owner, *local.runtime_id());
}

#[test]
fn routed_put_skips_draining_storage_and_authority() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let victim_transport = Arc::new(TestTransport::new("drain-fallback-victim-segment"));
    let survivor_transport = Arc::new(victim_transport.peer("drain-fallback-survivor-segment"));
    let writer_transport = Arc::new(victim_transport.peer("drain-fallback-writer-segment"));
    let reader_transport = Arc::new(victim_transport.peer("drain-fallback-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut victim = StoreClientBuilder::new(metadata.clone(), "drain-fallback-victim")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "true")
        .label("route_scope", "drain-fallback-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(victim_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("victim build should succeed");
    let survivor = StoreClientBuilder::new(metadata.clone(), "drain-fallback-survivor")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "true")
        .label("route_scope", "drain-fallback-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(survivor_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("survivor build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "drain-fallback-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "drain-fallback-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "drain-fallback-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "drain-fallback-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    victim
        .register_local_memory()
        .expect("victim memory should register");
    survivor
        .register_local_memory()
        .expect("survivor memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&victim, &survivor, &writer, &reader]);

    victim
        .enter_draining()
        .expect("victim should enter draining");

    let key = "drain-fallback-key";
    let value = b"drain-fallback-value";
    let route = writer
        .put_with_policy(
            key,
            value,
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .with_soft_pin(true)
                .preferred_storage_owners([
                    victim.runtime_id().storage_key(),
                    survivor.runtime_id().storage_key(),
                ]),
        )
        .expect("writer should skip draining victim and write survivor");

    assert_eq!(route.replicas[0].owner, *survivor.runtime_id());
    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new(format!("default::{key}"));
    assert!(
        authority_get(&namespace, &survivor.runtime_id().stable_id, &scoped_key)
            .expect("survivor authority should be readable")
            .is_some()
    );
    assert_eq!(
        reader.get(key).expect("reader should read survivor copy"),
        value
    );
}

#[test]
#[should_panic(expected = "cannot arm blocked CAS while a previous CAS is still blocked")]
fn blocking_cas_gate_rejects_rearm_while_previous_wait_is_active() {
    let blocking = BlockingCasMetadataBackend::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "default::blocked-cas-rearm-key",
    );
    let (lock, _) = &*blocking.gate;
    let mut state = lock.lock().expect("blocking CAS gate lock should succeed");
    state.entered = true;
    state.released = false;
    drop(state);

    blocking.arm_blocked_cas();
}

#[test]
fn true_client_shrink_waits_for_inflight_route_publish() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner,
        "default::shrink-inflight-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let store_a_transport = Arc::new(TestTransport::new("shrink-inflight-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("shrink-inflight-b-segment"));
    let router_transport = Arc::new(store_a_transport.peer("shrink-inflight-router-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("shrink-inflight-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "shrink-inflight-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "shrink-inflight-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "shrink-inflight-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata, "shrink-inflight-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
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

    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false)
        .preferred_storage_owners([store_a.runtime_id().storage_key()]);
    let store_a_runtime = store_a.runtime_id().clone();
    let key = "shrink-inflight-key";
    let value = b"shrink-inflight-value".to_vec();
    let writer_value = value.clone();
    blocking.arm_blocked_cas();
    let writer = std::thread::spawn(move || router.put_with_policy(key, &writer_value, &policy));

    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "writer should reach the blocked route publish point"
    );

    let release_blocker = blocking.clone();
    let releaser = std::thread::spawn(move || {
        sleep(Duration::from_millis(100));
        release_blocker.release_blocked_cas();
    });

    let started = Instant::now();
    let migrated = {
        let mut shrink_store = store_a;
        shrink_store
            .evacuate_owned_replicas()
            .expect("shrink should converge after the pending publish is released")
    };
    writer
        .join()
        .expect("writer thread should join")
        .expect("writer put should succeed");
    releaser.join().expect("release thread should join");
    assert!(
        started.elapsed() >= Duration::from_millis(80),
        "shrink should wait for the in-flight publish instead of returning immediately"
    );
    assert_eq!(migrated, 1);

    assert_eq!(
        reader
            .get(key)
            .expect("reader get after shrink should succeed"),
        value
    );
    let route = reader
        .query_route(key)
        .expect("route query should succeed after shrink")
        .expect("route should still exist after shrink");
    assert!(
        route
            .replicas
            .iter()
            .all(|replica| replica.owner != store_a_runtime),
        "route should migrate away from the drained runtime"
    );
}

#[test]
fn evacuate_owned_replicas_reads_the_draining_replica_source() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("drain-source-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("drain-source-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("drain-source-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("drain-source-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("drain-source-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut store_a = StoreClientBuilder::new(metadata.clone(), "drain-source-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "drain-source-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "drain-source-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "drain-source-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "drain-source-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    let good = b"drain-source-good";
    let bad = b"drain-source-bad!";
    let good_route = writer
        .put_with_policy(
            "drain-source-good-key",
            good,
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([store_a.runtime_id().storage_key()]),
        )
        .expect("good source write should land on store-a");
    let bad_route = writer
        .put_with_policy(
            "drain-source-bad-key",
            bad,
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([store_b.runtime_id().storage_key()]),
        )
        .expect("bad distractor write should land on store-b");

    store_a
        .route_directory
        .compare_and_swap_object_route(
            &store_a.lease,
            &good_route.key,
            Some(good_route.version),
            None,
        )
        .expect("good source route delete should succeed");
    store_a
        .route_directory
        .compare_and_swap_object_route(
            &store_a.lease,
            &bad_route.key,
            Some(bad_route.version),
            None,
        )
        .expect("bad source route delete should succeed");

    let target_key = "drain-source-target";
    let target_id = LogicalObjectId::new(NamespaceScope::default(), target_key);
    let mut bad_replica = bad_route.replicas[0].clone();
    bad_replica.priority = 0;
    bad_replica.checksum = None;
    let mut good_replica = good_route.replicas[0].clone();
    good_replica.priority = 1;
    good_replica.checksum = None;
    let mut route = ObjectRoute {
        key: ObjectKey::from_logical_id(&target_id),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: Some(mooncake_store_core::DEFAULT_QOS_TIER.to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: store_a.lease.compatibility.clone(),
        replicas: vec![bad_replica, good_replica],
    };
    mooncake_store_core::apply_route_identity(&mut route, &target_id);
    store_a
        .route_directory
        .compare_and_swap_object_route(&store_a.lease, &route.key, None, Some(&route))
        .expect("target route publish should succeed");
    store_a.storage_owner.track_route(&route);
    store_b.storage_owner.track_route(&route);

    assert_eq!(
        reader
            .get(target_key)
            .expect("precondition should read the primary distractor"),
        bad
    );

    let store_a_runtime = store_a.runtime_id().clone();
    let migrated = store_a
        .evacuate_owned_replicas()
        .expect("draining store should migrate from its own source replica");
    assert_eq!(migrated, 1);

    assert_eq!(
        reader
            .get(target_key)
            .expect("reader should see the draining replica payload after migration"),
        good
    );
    let migrated_route = reader
        .query_route(target_key)
        .expect("route query should succeed")
        .expect("route should exist");
    assert!(
        migrated_route
            .replicas
            .iter()
            .all(|replica| replica.owner != store_a_runtime),
        "drained owner should be removed from the migrated route"
    );
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
        "mooncake_store_segment_lifecycle_total{tenant=\"default\",action=\"mount_segment\",result=\"ok\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_segment_lifecycle_total{tenant=\"default\",action=\"retire_segment\",result=\"ok\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_rebalance_routes_total{tenant=\"default\",phase=\"migrate\",result=\"ok\"}"
    ));
    assert!(metrics
        .contains("mooncake_store_rebalance_bytes_total{tenant=\"default\",phase=\"migrate\"}"));
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
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(predecessor_transport)
        .transport_factory(predecessor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("predecessor build should succeed");
    let mut successor = StoreClientBuilder::new(metadata.clone(), "pin-upgrade-store")
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
fn hot_upgrade_evacuation_refreshes_expired_predecessor_lease() {
    let metadata = Arc::new(CountingMetadataBackend::with_expiring_live_clients(
        Arc::new(InMemoryMetadataBackend::new()),
    ));
    let predecessor_transport = Arc::new(TestTransport::new("expiring-upgrade-old-segment"));
    let predecessor_factory = predecessor_transport.factory();
    let successor_transport = Arc::new(predecessor_transport.peer("expiring-upgrade-new-segment"));
    let reader_transport = Arc::new(predecessor_transport.peer("expiring-upgrade-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let predecessor_expiry = now_ms().saturating_add(60);
    let long_expiry = now_ms().saturating_add(10_000);
    let mut predecessor = StoreClientBuilder::new(metadata.clone(), "expiring-upgrade-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(predecessor_transport)
        .transport_factory(predecessor_factory)
        .local_memory(storage_config())
        .build(predecessor_expiry)
        .expect("predecessor build should succeed");
    let mut successor = StoreClientBuilder::new(metadata.clone(), "expiring-upgrade-store")
        .state(ClientLifecycleState::Standby)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(successor_transport)
        .local_memory(storage_config())
        .build(long_expiry)
        .expect("successor build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "expiring-upgrade-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(long_expiry)
        .expect("reader build should succeed");

    predecessor
        .register_local_memory()
        .expect("predecessor memory should register");
    successor
        .register_local_memory()
        .expect("successor memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    predecessor
        .put("expiring-upgrade-key", b"expiring-upgrade-payload")
        .expect("seed put should succeed");
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

    sleep(Duration::from_millis(80));
    assert!(
        metadata
            .list_live_clients()
            .expect("live clients should list")
            .into_iter()
            .all(|lease| lease.runtime != *predecessor.runtime_id()),
        "predecessor lease should age out before evacuation begins"
    );

    let successor_runtime = successor.runtime_id().clone();
    let migrated = predecessor
        .evacuate_owned_replicas_to_runtime(&successor_runtime)
        .expect("evacuation should refresh the predecessor lease before migrating");
    assert_eq!(migrated, 1);

    let route = reader
        .query_route("expiring-upgrade-key")
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
            .get("expiring-upgrade-key")
            .expect("reader get should succeed"),
        b"expiring-upgrade-payload"
    );
}

#[test]
fn internal_allocator_and_store_state_cover_edge_cases() {
    let owner = ClientRuntimeId::new("writer", ClientEpoch(7));
    let primary = SegmentAnnouncement {
        owner: owner.clone(),
        segment_name: SegmentName::new("alloc-primary"),
        transport_endpoint: None,
        transport_segment_descriptor: None,
        capacity_bytes: 128,
        used_bytes: 0,
        target_chunks: Vec::new(),
        state: SegmentLifecycleState::Active,
        alignment_bytes: 16,
        tags: vec!["dram".to_string()],
    };
    let secondary = SegmentAnnouncement {
        segment_name: SegmentName::new("alloc-secondary"),
        transport_endpoint: None,
        transport_segment_descriptor: None,
        capacity_bytes: 64,
        ..primary.clone()
    };
    let draining = SegmentAnnouncement {
        segment_name: SegmentName::new("alloc-draining"),
        transport_endpoint: None,
        transport_segment_descriptor: None,
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
        transport_endpoint: None,
        transport_segment_descriptor: None,
        used_bytes: 64,
        target_chunks: Vec::new(),
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

    let pending_deadline = now_ms().saturating_add(1_000);
    allocator.mark_pending_allocation(
        &primary.segment_name,
        reused.offset_bytes,
        reused.length_bytes,
        pending_deadline,
    );
    assert!(allocator
        .pending_allocations(now_ms())
        .contains(&AllocationSpan {
            segment_name: primary.segment_name.clone(),
            offset_bytes: reused.offset_bytes,
            length_bytes: reused.length_bytes,
        }));
    let pending_route = mooncake_store_core::ObjectRoute {
        key: ObjectKey::new("default::allocator-pending"),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("allocator-pending".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: owner.clone(),
            segment_name: primary.segment_name.clone(),
            offset: Some(0),
            segment_offset: reused.offset_bytes,
            length: reused.length_bytes,
            checksum: None,
            tier: mooncake_store_core::ReplicaTier::Dram,
            priority: 0,
        }],
    };
    allocator.clear_pending_route(&pending_route, &owner);
    assert!(allocator.pending_allocations(now_ms()).is_empty());

    let shared_allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
    shared_allocator.lock().upsert(&primary);
    let adapter = LocalAllocatorAdapter {
        runtime: owner.clone(),
        allocator: shared_allocator.clone(),
        storage_owner: test_storage_owner_state(&owner, shared_allocator.clone()),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
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
        policy_rank: 1,
        tenant: "tenant-a".to_string(),
        qos_tier: "default".to_string(),
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-a"),
        offset_bytes: 0,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 50,
        policy_rank: 0,
        tenant: "tenant-b".to_string(),
        qos_tier: "bronze".to_string(),
        storage_runtime: owner,
        segment_name: SegmentName::new("seg-b"),
        offset_bytes: 16,
        length_bytes: 8,
    });
    assert_eq!(state.take_due_reclaims(10).len(), 1);
    assert_eq!(state.pending_reclaims.len(), 1);
}

#[test]
fn drain_stale_release_waits_for_pending_publish_deadline() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("pending-deadline-store"));
    let client = StoreClientBuilder::new(metadata, "pending-deadline-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .request_timeout(Duration::from_millis(25))
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("memory registration should succeed");

    let reservation = client
        .reserve_segment_allocation(client.runtime_id(), None, 32, true)
        .expect("reservation should succeed");
    let primary = client.segment_name().expect("primary segment should exist");
    client
        .drain_segment_internal(&primary, true)
        .expect("segment drain should succeed");
    assert_eq!(
        client
            .release_stale_local_allocations(&BTreeSet::new())
            .expect("fresh pending reservation should not be released"),
        0
    );

    sleep(Duration::from_millis(400));
    assert_eq!(
        client
            .release_stale_local_allocations(&BTreeSet::new())
            .expect("expired pending reservation should be released"),
        1
    );
    assert!(
        client
            .allocator
            .lock()
            .pending_allocations(now_ms())
            .is_empty(),
        "expired pending reservation should no longer block drain"
    );
    assert_eq!(reservation.length_bytes, 32);
}

#[test]
fn local_allocator_pending_window_respects_publish_and_timeout() {
    let owner = ClientRuntimeId::new("pending-owner", ClientEpoch(11));
    let primary = SegmentAnnouncement {
        owner: owner.clone(),
        segment_name: SegmentName::new("pending-primary"),
        transport_endpoint: None,
        transport_segment_descriptor: None,
        capacity_bytes: 128,
        used_bytes: 0,
        target_chunks: Vec::new(),
        state: SegmentLifecycleState::Active,
        alignment_bytes: 16,
        tags: vec!["dram".to_string()],
    };
    let mut allocator = LocalAllocatorState::default();
    allocator.upsert(&primary);

    let published = allocator
        .reserve_any(&owner, 17)
        .expect("reservation should succeed");
    allocator.mark_pending_reservation(&published, now_ms().saturating_add(200));
    assert!(
        allocator
            .stale_allocations(&BTreeSet::new(), now_ms())
            .is_empty(),
        "fresh pending allocation should not be stale"
    );
    let route = mooncake_store_core::ObjectRoute {
        key: ObjectKey::new("default::pending-route"),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("pending-route".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: owner.clone(),
            segment_name: published.segment_name.clone(),
            offset: Some(0),
            segment_offset: published.offset_bytes,
            length: published.length_bytes,
            checksum: None,
            tier: mooncake_store_core::ReplicaTier::Dram,
            priority: 0,
        }],
    };
    allocator.clear_pending_route(&route, &owner);
    assert!(
        allocator.pending_allocations(now_ms()).is_empty(),
        "published route should clear the pending allocation window"
    );

    let orphaned = allocator
        .reserve_any(&owner, 17)
        .expect("second reservation should succeed");
    allocator.mark_pending_reservation(&orphaned, now_ms().saturating_add(50));
    assert!(
        !allocator
            .stale_allocations(&BTreeSet::new(), now_ms())
            .contains(&AllocationSpan {
                segment_name: orphaned.segment_name.clone(),
                offset_bytes: orphaned.offset_bytes,
                length_bytes: orphaned.length_bytes,
            }),
        "fresh orphan should still be protected by the pending window"
    );
    sleep(Duration::from_millis(80));
    assert!(allocator
        .stale_allocations(&BTreeSet::new(), now_ms())
        .contains(&AllocationSpan {
            segment_name: orphaned.segment_name,
            offset_bytes: orphaned.offset_bytes,
            length_bytes: orphaned.length_bytes,
        }));
}

#[test]
fn qos_tier_reclaims_low_priority_before_high_priority() {
    let mut state = StoreState::default();
    let owner = ClientRuntimeId::new("owner-a", ClientEpoch(1));
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 3,
        tenant: "tenant-a".to_string(),
        qos_tier: "critical".to_string(),
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-critical"),
        offset_bytes: 0,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 2,
        tenant: "tenant-a".to_string(),
        qos_tier: "gold".to_string(),
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-gold"),
        offset_bytes: 8,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 0,
        tenant: "tenant-a".to_string(),
        qos_tier: "bronze".to_string(),
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-bronze"),
        offset_bytes: 16,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 1,
        tenant: "tenant-a".to_string(),
        qos_tier: "default".to_string(),
        storage_runtime: owner,
        segment_name: SegmentName::new("seg-default"),
        offset_bytes: 24,
        length_bytes: 8,
    });

    let due = state.take_due_reclaims(10);
    assert_eq!(due.len(), 4);
    assert_eq!(due[0].segment_name, SegmentName::new("seg-bronze"));
    assert_eq!(due[1].segment_name, SegmentName::new("seg-default"));
    assert_eq!(due[2].segment_name, SegmentName::new("seg-gold"));
    assert_eq!(due[3].segment_name, SegmentName::new("seg-critical"));
}

#[test]
fn due_reclaims_are_sorted_by_policy_rank_then_due_time() {
    let mut state = StoreState::default();
    let owner = ClientRuntimeId::new("owner-a", ClientEpoch(1));
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 2,
        tenant: "tenant-a".to_string(),
        qos_tier: "gold".to_string(),
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-a"),
        offset_bytes: 0,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 0,
        tenant: "tenant-b".to_string(),
        qos_tier: "bronze".to_string(),
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-b"),
        offset_bytes: 8,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        policy_rank: 0,
        tenant: "tenant-b".to_string(),
        qos_tier: "default".to_string(),
        storage_runtime: owner,
        segment_name: SegmentName::new("seg-c"),
        offset_bytes: 16,
        length_bytes: 8,
    });

    let due = state.take_due_reclaims(10);
    assert_eq!(due.len(), 3);
    assert_eq!(due[0].segment_name, SegmentName::new("seg-b"));
    assert_eq!(due[1].segment_name, SegmentName::new("seg-c"));
    assert_eq!(due[2].segment_name, SegmentName::new("seg-a"));
}

#[test]
fn batch_get_into_returns_partial_results_when_some_keys_miss() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let storage_transport = Arc::new(TestTransport::new("partial-resolve-storage-segment"));
    let router_transport = Arc::new(storage_transport.peer("partial-resolve-router-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "partial-resolve-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "partial-resolve-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    wait_for_membership_convergence(&[&storage, &router]);

    let payload = b"partial-resolve-payload";
    router
        .put("existing-key", payload)
        .expect("put should succeed");

    let mut buf_a = vec![0u8; 64];
    let mut buf_b = vec![0u8; 64];
    let mut requests = vec![
        GetRequest::new("existing-key", &mut buf_a),
        GetRequest::new("missing-key", &mut buf_b),
    ];
    let sizes = router
        .batch_get_into(&mut requests)
        .expect("batch_get_into should return Ok even when some keys are missing");
    assert_eq!(sizes[0], payload.len(), "existing key should return data");
    assert_eq!(sizes[1], 0, "missing key should return zero bytes");
    assert_eq!(&buf_a[..sizes[0]], payload);
}

#[test]
fn batch_get_returns_empty_for_missing_keys_in_partial_batch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let storage_transport = Arc::new(TestTransport::new("partial-batch-storage-segment"));
    let router_transport = Arc::new(storage_transport.peer("partial-batch-router-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "partial-batch-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "partial-batch-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    wait_for_membership_convergence(&[&storage, &router]);

    let payload = b"partial-batch-payload";
    router
        .put("batch-existing", payload)
        .expect("put should succeed");

    let result = router
        .batch_get(&[
            ObjectRef::new("batch-existing"),
            ObjectRef::new("batch-missing"),
        ])
        .expect("batch_get should return Ok for partial results");
    assert_eq!(result[0], payload, "existing key should return data");
    assert!(result[1].is_empty(), "missing key should return empty vec");
}

#[test]
fn single_get_still_returns_not_found_for_missing_key() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let storage_transport = Arc::new(TestTransport::new("single-miss-storage-segment"));
    let router_transport = Arc::new(storage_transport.peer("single-miss-router-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "single-miss-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "single-miss-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    wait_for_membership_convergence(&[&storage, &router]);

    let error = router.get("nonexistent-key").unwrap_err();
    assert!(
        matches!(error, StoreError::NotFound(_)),
        "single get of missing key should return NotFound, got {error}"
    );
}

#[test]
fn batch_put_from_overwrites_stale_route_when_readable_filter_excludes_replica_owner() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let dead_transport = Arc::new(TestTransport::new("stale-overwrite-dead-segment"));
    let writer_transport = Arc::new(dead_transport.peer("stale-overwrite-writer-segment"));

    let dead_store = StoreClientBuilder::new(metadata.clone(), "stale-overwrite-dead")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(dead_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("dead store build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "stale-overwrite-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    dead_store
        .register_local_memory()
        .expect("dead store memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_membership_convergence(&[&dead_store, &writer]);

    let source = b"stale-overwrite-initial-data-pad";
    writer
        .register_buffer(source.as_ptr() as *mut c_void, source.len())
        .expect("source buffer should register");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(dead_store.runtime_id().storage_key());
    let initial_routes = writer
        .batch_put_from(&[PutFromRequest::new(
            "stale-overwrite-key",
            source.as_ptr().cast(),
            source.len(),
        )
        .replication(policy)])
        .expect("initial put should succeed");
    assert_eq!(initial_routes.len(), 1);
    assert_eq!(
        initial_routes[0].replicas[0].owner,
        *dead_store.runtime_id(),
        "initial route should point to dead_store"
    );

    let namespace = metadata.route_namespace();
    mooncake_store_route::update_readable_filter(
        &namespace,
        Some(BTreeSet::from([writer.runtime_id().clone()])),
    );

    let new_source = b"stale-overwrite-replaced-data!!";
    writer
        .register_buffer(new_source.as_ptr() as *mut c_void, new_source.len())
        .expect("new source buffer should register");

    let local_policy = ReplicationPolicy::new().prefer_local(true);
    let new_routes = writer
        .batch_put_from(&[PutFromRequest::new(
            "stale-overwrite-key",
            new_source.as_ptr().cast(),
            new_source.len(),
        )
        .replication(local_policy)])
        .expect("put with stale route should succeed via stale overwrite");
    assert_eq!(new_routes.len(), 1);
    assert_eq!(
        new_routes[0].replicas[0].owner,
        *writer.runtime_id(),
        "stale route should be overwritten to point to writer's local storage"
    );
    assert!(
        new_routes[0].version > initial_routes[0].version,
        "overwritten route version should be higher than stale version"
    );
}

#[test]
fn batch_put_from_succeeds_when_authority_has_version_floor_but_no_route() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("floor-test-segment"));

    let store = StoreClientBuilder::new(metadata.clone(), "floor-test-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");

    store
        .register_local_memory()
        .expect("memory should register");
    wait_for_membership_convergence(&[&store]);

    let source = b"floor-test-initial-data-padding";
    store
        .register_buffer(source.as_ptr() as *mut c_void, source.len())
        .expect("source buffer should register");

    let initial_routes = store
        .batch_put_from(&[PutFromRequest::new(
            "floor-test-key",
            source.as_ptr().cast(),
            source.len(),
        )])
        .expect("initial put should succeed");
    assert_eq!(initial_routes.len(), 1);
    let initial_version = initial_routes[0].version;

    store
        .remove("floor-test-key", false)
        .expect("remove should succeed");
    let exists_after_remove = store
        .is_exist("floor-test-key")
        .expect("is_exist should succeed");
    assert!(!exists_after_remove, "route should be gone after remove");

    let namespace = metadata.route_namespace();
    mooncake_store_route::update_readable_filter(
        &namespace,
        Some(BTreeSet::from([store.runtime_id().clone()])),
    );

    let new_source = b"floor-test-new-data-after-floor";
    store
        .register_buffer(new_source.as_ptr() as *mut c_void, new_source.len())
        .expect("new source buffer should register");

    let new_routes = store
        .batch_put_from(&[PutFromRequest::new(
            "floor-test-key",
            new_source.as_ptr().cast(),
            new_source.len(),
        )])
        .expect("put after remove should succeed via version_floor in CAS response");
    assert_eq!(new_routes.len(), 1);
    assert!(
        new_routes[0].version > initial_version,
        "new route version ({:?}) should exceed initial version ({:?}) due to floor",
        new_routes[0].version,
        initial_version,
    );
}

#[test]
fn put_succeeds_when_authority_has_version_floor_but_no_route() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("put-floor-segment"));

    let store = StoreClientBuilder::new(metadata.clone(), "put-floor-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");

    store
        .register_local_memory()
        .expect("memory should register");
    wait_for_membership_convergence(&[&store]);

    let initial_data = b"put-floor-initial-data-padding";
    let initial_route = store
        .put("put-floor-key", initial_data)
        .expect("initial put should succeed");
    let initial_version = initial_route.version;

    store
        .remove("put-floor-key", false)
        .expect("remove should succeed");
    assert!(
        !store
            .is_exist("put-floor-key")
            .expect("is_exist should succeed"),
        "route should be gone after remove"
    );

    let new_data = b"put-floor-new-data-after-floor-";
    let new_route = store
        .put("put-floor-key", new_data)
        .expect("put after remove should succeed via version_floor in CAS response");
    assert!(
        new_route.version > initial_version,
        "new route version ({:?}) should exceed initial version ({:?}) due to floor",
        new_route.version,
        initial_version,
    );
}

#[test]
fn batch_put_from_version_floor_works_with_remote_storage_authority() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("floor-remote-store-segment"));
    let writer_transport = Arc::new(store_transport.peer("floor-remote-writer-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store = StoreClientBuilder::new(metadata.clone(), "floor-remote-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "floor-remote-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_membership_convergence(&[&store, &writer]);

    let source = b"floor-remote-initial-data-pad!!";
    writer
        .register_buffer(source.as_ptr() as *mut c_void, source.len())
        .expect("source buffer should register");

    let initial_routes = writer
        .batch_put_from(&[PutFromRequest::new(
            "floor-remote-key",
            source.as_ptr().cast(),
            source.len(),
        )])
        .expect("initial put should succeed");
    assert_eq!(initial_routes.len(), 1);
    let initial_version = initial_routes[0].version;
    assert_eq!(
        initial_routes[0].replicas[0].owner,
        *store.runtime_id(),
        "initial route should point to the remote storage store"
    );

    writer
        .remove("floor-remote-key", false)
        .expect("remove should succeed");

    let new_source = b"floor-remote-new-data-after-fl!";
    writer
        .register_buffer(new_source.as_ptr() as *mut c_void, new_source.len())
        .expect("new source buffer should register");

    let new_routes = writer
        .batch_put_from(&[PutFromRequest::new(
            "floor-remote-key",
            new_source.as_ptr().cast(),
            new_source.len(),
        )])
        .expect("put after remove should succeed via version_floor in CAS response");
    assert_eq!(new_routes.len(), 1);
    assert!(
        new_routes[0].version > initial_version,
        "new route version ({:?}) should exceed initial version ({:?}) due to floor",
        new_routes[0].version,
        initial_version,
    );
}

#[test]
fn put_version_floor_works_with_remote_storage_authority() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("put-floor-remote-store-seg"));
    let writer_transport = Arc::new(store_transport.peer("put-floor-remote-writer-seg"));

    let store = StoreClientBuilder::new(metadata.clone(), "put-floor-remote-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(store_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "put-floor-remote-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_membership_convergence(&[&store, &writer]);

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(store.runtime_id().storage_key());
    let initial_data = b"put-floor-remote-initial-data!";
    let initial_route = writer
        .put_with_policy("put-floor-remote-key", initial_data, &policy)
        .expect("initial put should succeed");
    let initial_version = initial_route.version;

    writer
        .remove("put-floor-remote-key", false)
        .expect("remove should succeed");

    let new_data = b"put-floor-remote-new-data-pad!";
    let new_route = writer
        .put_with_policy("put-floor-remote-key", new_data, &policy)
        .expect("put after remove should succeed via version_floor in CAS response");
    assert!(
        new_route.version > initial_version,
        "new route version ({:?}) should exceed initial version ({:?}) due to floor",
        new_route.version,
        initial_version,
    );
}

#[test]
fn batch_is_exist_returns_false_when_readable_filter_excludes_all_replica_owners() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let dead_transport = Arc::new(TestTransport::new("is-exist-filter-dead-segment"));
    let reader_transport = Arc::new(dead_transport.peer("is-exist-filter-reader-seg"));

    let dead_store = StoreClientBuilder::new(metadata.clone(), "is-exist-filter-dead")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(dead_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("dead store build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "is-exist-filter-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    dead_store
        .register_local_memory()
        .expect("dead store memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&dead_store, &reader]);

    let data = b"is-exist-filter-test-data-pad!";
    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(dead_store.runtime_id().storage_key());
    dead_store
        .put_with_policy("is-exist-filter-key", data, &policy)
        .expect("initial put should succeed");

    assert!(
        reader
            .is_exist("is-exist-filter-key")
            .expect("is_exist should succeed"),
        "route should be visible before readable filter activation"
    );

    let namespace = metadata.route_namespace();
    mooncake_store_route::update_readable_filter(
        &namespace,
        Some(BTreeSet::from([reader.runtime_id().clone()])),
    );

    assert!(
        !reader
            .is_exist("is-exist-filter-key")
            .expect("is_exist should succeed"),
        "route should be invisible when readable filter excludes all replica owners"
    );

    mooncake_store_route::update_readable_filter(&namespace, None);

    assert!(
        !reader
            .is_exist("is-exist-filter-key")
            .expect("is_exist should succeed"),
        "route should remain absent after eviction even when readable filter is deactivated"
    );
}

#[test]
fn is_exist_evicts_unreadable_route_and_sets_version_floor() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let dead_transport = Arc::new(TestTransport::new("evict-floor-dead-seg"));
    let writer_transport = Arc::new(dead_transport.peer("evict-floor-writer-seg"));

    let dead_store = StoreClientBuilder::new(metadata.clone(), "evict-floor-dead")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(dead_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("dead store build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "evict-floor-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    dead_store
        .register_local_memory()
        .expect("dead store memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_membership_convergence(&[&dead_store, &writer]);

    let data = b"evict-floor-test-data-payload!";
    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(dead_store.runtime_id().storage_key());
    dead_store
        .put_with_policy("evict-floor-key", data, &policy)
        .expect("initial put should succeed");

    let initial_route = writer
        .query_route("evict-floor-key")
        .expect("get route should succeed")
        .expect("route should exist after put");
    let initial_version = initial_route.version;

    let namespace = metadata.route_namespace();
    mooncake_store_route::update_readable_filter(
        &namespace,
        Some(BTreeSet::from([writer.runtime_id().clone()])),
    );

    assert!(
        !writer
            .is_exist("evict-floor-key")
            .expect("is_exist should succeed"),
        "is_exist should return false when all replicas are unreadable"
    );

    mooncake_store_route::update_readable_filter(&namespace, None);

    let new_data = b"evict-floor-test-data-updated";
    writer
        .put("evict-floor-key", new_data)
        .expect("re-put after eviction should succeed");

    let new_route = writer
        .query_route("evict-floor-key")
        .expect("get route should succeed")
        .expect("route should exist after re-put");
    assert!(
        new_route.version > initial_version,
        "re-put version {:?} must exceed evicted version {:?} due to version_floor",
        new_route.version,
        initial_version
    );
}

mod adversarial;
mod fault_injection_prop;
mod lifecycle_tests;
mod namespace_adversarial;
mod namespace_isolation_tests;
mod perf_invariant_tests;
mod quota_prop;
mod route_migration_tests;
mod routing_prop;
mod routing_tests;
mod store_client_tests;
mod transport_prop;
mod unit_tests;
