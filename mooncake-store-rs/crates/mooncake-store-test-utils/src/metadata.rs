use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    LogicalObjectId, MetadataBackend, NamespaceScope, ObjectKey, ObjectRoute, Result,
    ReuseIdentity, RoutePolicy, RoutePolicyDomain, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, SegmentReservation, StoreError, TenantObjectAccounting,
    TenantPolicy, TenantPolicyScope, TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome,
    TenantQuotaFinalizeRequest, TenantQuotaReservation, TenantQuotaReservationOutcome,
    TenantQuotaReservationRequest, TenantQuotaState,
};

// ---------------------------------------------------------------------------
// OperationCounts — per-method call counters
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct OperationCounts {
    pub upsert_client_lease: AtomicU64,
    pub allocate_client_lease: AtomicU64,
    pub update_client_state: AtomicU64,
    pub list_live_clients: AtomicU64,
    pub publish_segment: AtomicU64,
    pub unpublish_segment: AtomicU64,
    pub list_segments: AtomicU64,
    pub update_segment_state: AtomicU64,
    pub reserve_segment: AtomicU64,
    pub release_segment: AtomicU64,
    pub get_object_route: AtomicU64,
    pub list_object_routes: AtomicU64,
    pub compare_and_swap_object_route: AtomicU64,
    pub get_route_policy: AtomicU64,
    pub put_route_policy_if_absent: AtomicU64,
    pub put_route_policy: AtomicU64,
    pub delete_route_policy: AtomicU64,
    pub list_route_policies: AtomicU64,
    pub get_tenant_policy: AtomicU64,
    pub list_tenant_policies: AtomicU64,
    pub put_tenant_policy: AtomicU64,
    pub delete_tenant_policy: AtomicU64,
    pub get_tenant_quota_state: AtomicU64,
    pub get_tenant_object_accounting: AtomicU64,
    pub list_tenant_quota_reservations: AtomicU64,
    pub reserve_tenant_quota: AtomicU64,
    pub finalize_tenant_quota: AtomicU64,
    pub abort_tenant_quota: AtomicU64,
    pub put_handoff: AtomicU64,
    pub get_handoff: AtomicU64,
}

impl OperationCounts {
    pub fn load(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// CountingMetadataBackend — delegates all ops and increments per-method counters
// ---------------------------------------------------------------------------

pub struct CountingMetadataBackend {
    inner: Arc<dyn MetadataBackend>,
    pub counts: Arc<OperationCounts>,
}

impl CountingMetadataBackend {
    pub fn wrap(inner: Arc<dyn MetadataBackend>) -> (Arc<Self>, Arc<OperationCounts>) {
        let counts = Arc::new(OperationCounts::default());
        let backend = Arc::new(Self {
            inner,
            counts: Arc::clone(&counts),
        });
        (backend, counts)
    }
}

impl MetadataBackend for CountingMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        self.counts
            .upsert_client_lease
            .fetch_add(1, Ordering::Relaxed);
        self.inner.upsert_client_lease(lease)
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        self.counts
            .allocate_client_lease
            .fetch_add(1, Ordering::Relaxed);
        self.inner.allocate_client_lease(template)
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        self.counts
            .update_client_state
            .fetch_add(1, Ordering::Relaxed);
        self.inner.update_client_state(runtime, next)
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        self.counts
            .list_live_clients
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_live_clients()
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        self.counts.publish_segment.fetch_add(1, Ordering::Relaxed);
        self.inner.publish_segment(segment)
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        self.counts
            .unpublish_segment
            .fetch_add(1, Ordering::Relaxed);
        self.inner.unpublish_segment(owner, segment)
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        self.counts.list_segments.fetch_add(1, Ordering::Relaxed);
        self.inner.list_segments(owner)
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        self.counts
            .update_segment_state
            .fetch_add(1, Ordering::Relaxed);
        self.inner.update_segment_state(owner, segment, next)
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        self.counts.reserve_segment.fetch_add(1, Ordering::Relaxed);
        self.inner.reserve_segment(owner, segment, length_bytes)
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        self.counts.release_segment.fetch_add(1, Ordering::Relaxed);
        self.inner
            .release_segment(owner, segment, offset_bytes, length_bytes)
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        self.counts.get_object_route.fetch_add(1, Ordering::Relaxed);
        self.inner.get_object_route(key)
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        self.counts
            .list_object_routes
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_object_routes()
    }

    fn list_object_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        self.inner.list_object_routes_in_scope(scope)
    }

    fn get_object_route_by_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>> {
        self.inner.get_object_route_by_id(object_id)
    }

    fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        self.inner.list_reuse_candidates(reuse)
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.counts
            .compare_and_swap_object_route
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .compare_and_swap_object_route(key, expected, next)
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        self.counts.get_route_policy.fetch_add(1, Ordering::Relaxed);
        self.inner.get_route_policy(domain)
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        self.counts
            .put_route_policy_if_absent
            .fetch_add(1, Ordering::Relaxed);
        self.inner.put_route_policy_if_absent(domain, policy)
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        self.counts.put_route_policy.fetch_add(1, Ordering::Relaxed);
        self.inner.put_route_policy(domain, policy)
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        self.counts
            .delete_route_policy
            .fetch_add(1, Ordering::Relaxed);
        self.inner.delete_route_policy(domain)
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        self.counts
            .list_route_policies
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_route_policies()
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        self.counts
            .get_tenant_policy
            .fetch_add(1, Ordering::Relaxed);
        self.inner.get_tenant_policy(scope)
    }

    fn list_tenant_policies(&self) -> Result<Vec<TenantPolicy>> {
        self.counts
            .list_tenant_policies
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_tenant_policies()
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        self.counts
            .put_tenant_policy
            .fetch_add(1, Ordering::Relaxed);
        self.inner.put_tenant_policy(policy, expected_version)
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        self.counts
            .delete_tenant_policy
            .fetch_add(1, Ordering::Relaxed);
        self.inner.delete_tenant_policy(scope, expected_version)
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        self.counts
            .get_tenant_quota_state
            .fetch_add(1, Ordering::Relaxed);
        self.inner.get_tenant_quota_state(scope)
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        self.counts
            .get_tenant_object_accounting
            .fetch_add(1, Ordering::Relaxed);
        self.inner.get_tenant_object_accounting(key)
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        self.counts
            .list_tenant_quota_reservations
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_tenant_quota_reservations(scope)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        self.counts
            .reserve_tenant_quota
            .fetch_add(1, Ordering::Relaxed);
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        self.counts
            .finalize_tenant_quota
            .fetch_add(1, Ordering::Relaxed);
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        self.counts
            .abort_tenant_quota
            .fetch_add(1, Ordering::Relaxed);
        self.inner.abort_tenant_quota(reservation_id)
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        self.counts.put_handoff.fetch_add(1, Ordering::Relaxed);
        self.inner.put_handoff(handoff)
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        self.counts.get_handoff.fetch_add(1, Ordering::Relaxed);
        self.inner.get_handoff(stable_id)
    }
}

// ---------------------------------------------------------------------------
// FaultyMetadataBackend — returns errors for all calls after the Nth
// ---------------------------------------------------------------------------

pub struct FaultyMetadataBackend {
    inner: Arc<dyn MetadataBackend>,
    fail_after: u64,
    call_count: AtomicU64,
}

impl FaultyMetadataBackend {
    /// Wraps `inner`. After `fail_after` successful calls, every subsequent
    /// call returns `StoreError::Metadata("injected fault")`.
    /// Pass `fail_after = 0` to make every call fail immediately.
    pub fn wrap(inner: Arc<dyn MetadataBackend>, fail_after: u64) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail_after,
            call_count: AtomicU64::new(0),
        })
    }

    fn check(&self) -> Result<()> {
        let n = self.call_count.fetch_add(1, Ordering::Relaxed);
        if n >= self.fail_after {
            return Err(StoreError::Metadata("injected fault".to_string()));
        }
        Ok(())
    }
}

impl MetadataBackend for FaultyMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        self.check()?;
        self.inner.upsert_client_lease(lease)
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        self.check()?;
        self.inner.allocate_client_lease(template)
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        self.check()?;
        self.inner.update_client_state(runtime, next)
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        self.check()?;
        self.inner.list_live_clients()
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        self.check()?;
        self.inner.publish_segment(segment)
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        self.check()?;
        self.inner.unpublish_segment(owner, segment)
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        self.check()?;
        self.inner.list_segments(owner)
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        self.check()?;
        self.inner.update_segment_state(owner, segment, next)
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        self.check()?;
        self.inner.reserve_segment(owner, segment, length_bytes)
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        self.check()?;
        self.inner
            .release_segment(owner, segment, offset_bytes, length_bytes)
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        self.check()?;
        self.inner.get_object_route(key)
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        self.check()?;
        self.inner.list_object_routes()
    }

    fn list_object_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        self.check()?;
        self.inner.list_object_routes_in_scope(scope)
    }

    fn get_object_route_by_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>> {
        self.check()?;
        self.inner.get_object_route_by_id(object_id)
    }

    fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        self.check()?;
        self.inner.list_reuse_candidates(reuse)
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.check()?;
        self.inner
            .compare_and_swap_object_route(key, expected, next)
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        self.check()?;
        self.inner.get_route_policy(domain)
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        self.check()?;
        self.inner.put_route_policy_if_absent(domain, policy)
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        self.check()?;
        self.inner.put_route_policy(domain, policy)
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        self.check()?;
        self.inner.delete_route_policy(domain)
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        self.check()?;
        self.inner.list_route_policies()
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        self.check()?;
        self.inner.get_tenant_policy(scope)
    }

    fn list_tenant_policies(&self) -> Result<Vec<TenantPolicy>> {
        self.check()?;
        self.inner.list_tenant_policies()
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        self.check()?;
        self.inner.put_tenant_policy(policy, expected_version)
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        self.check()?;
        self.inner.delete_tenant_policy(scope, expected_version)
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        self.check()?;
        self.inner.get_tenant_quota_state(scope)
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        self.check()?;
        self.inner.get_tenant_object_accounting(key)
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        self.check()?;
        self.inner.list_tenant_quota_reservations(scope)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        self.check()?;
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        self.check()?;
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        self.check()?;
        self.inner.abort_tenant_quota(reservation_id)
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        self.check()?;
        self.inner.put_handoff(handoff)
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        self.check()?;
        self.inner.get_handoff(stable_id)
    }
}
