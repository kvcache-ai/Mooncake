use std::sync::Arc;
use std::time::Instant;

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    LogicalObjectId, MetadataBackend, NamespaceScope, ObjectKey, ObjectRoute, Result,
    ReuseIdentity, RoutePolicy, RoutePolicyDomain, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, SegmentReservation, StoreError, TenantObjectAccounting,
    TenantPolicy, TenantPolicyScope, TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome,
    TenantQuotaFinalizeRequest, TenantQuotaReservation, TenantQuotaReservationOutcome,
    TenantQuotaReservationRequest, TenantQuotaState,
};

use super::registry;

pub(crate) fn observe_metadata_backend(
    metadata: Arc<dyn MetadataBackend>,
) -> Arc<dyn MetadataBackend> {
    observe_metadata_backend_with_registry(metadata, registry::global_metrics_registry().clone())
}

pub(crate) fn observe_metadata_backend_with_registry(
    metadata: Arc<dyn MetadataBackend>,
    registry: registry::SharedMetricsRegistry,
) -> Arc<dyn MetadataBackend> {
    if metadata.metrics_observed() {
        return metadata;
    }
    Arc::new(ObservedMetadataBackend {
        backend: metadata.backend_kind(),
        inner: metadata,
        registry,
    })
}

struct ObservedMetadataBackend {
    backend: &'static str,
    inner: Arc<dyn MetadataBackend>,
    registry: registry::SharedMetricsRegistry,
}

impl ObservedMetadataBackend {
    fn observe<T>(&self, operation: &'static str, call: impl FnOnce() -> Result<T>) -> Result<T> {
        let _inflight =
            MetadataInflightGuard::enter(self.backend, operation, self.registry.clone());
        let start = Instant::now();
        let result = call();
        registry::record_metadata_operation_with_registry(
            &self.registry,
            self.backend,
            operation,
            metadata_result_label(&result),
            start.elapsed(),
        );
        result
    }
}

struct MetadataInflightGuard {
    backend: &'static str,
    operation: &'static str,
    registry: registry::SharedMetricsRegistry,
}

impl MetadataInflightGuard {
    fn enter(
        backend: &'static str,
        operation: &'static str,
        registry: registry::SharedMetricsRegistry,
    ) -> Self {
        registry::increment_metadata_inflight_with_registry(&registry, backend, operation);
        Self {
            backend,
            operation,
            registry,
        }
    }
}

impl Drop for MetadataInflightGuard {
    fn drop(&mut self) {
        registry::decrement_metadata_inflight_with_registry(
            &self.registry,
            self.backend,
            self.operation,
        );
    }
}

pub(super) fn metadata_result_label<T>(result: &Result<T>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(StoreError::NotFound(_)) => "not_found",
        Err(StoreError::Conflict(_)) => "conflict",
        Err(StoreError::QuotaExceeded { .. }) => "quota_exceeded",
        Err(StoreError::InvalidState(_)) => "invalid_state",
        Err(StoreError::StaleEpoch(_)) => "stale_epoch",
        Err(StoreError::Unsupported(_)) => "unsupported",
        Err(StoreError::Allocator(_)) => "allocator_error",
        Err(StoreError::Metadata(_)) => "metadata_error",
        Err(StoreError::Transport(_)) => "transport_error",
    }
}

impl MetadataBackend for ObservedMetadataBackend {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn backend_kind(&self) -> &'static str {
        self.backend
    }

    fn metrics_observed(&self) -> bool {
        true
    }

    fn for_tenant(&self, tenant: &str) -> Option<Arc<dyn MetadataBackend>> {
        self.inner
            .for_tenant(tenant)
            .map(|metadata| observe_metadata_backend_with_registry(metadata, self.registry.clone()))
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        self.observe("upsert_client_lease", || {
            self.inner.upsert_client_lease(lease)
        })
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        self.observe("allocate_client_lease", || {
            self.inner.allocate_client_lease(template)
        })
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        self.observe("update_client_state", || {
            self.inner.update_client_state(runtime, next)
        })
    }

    fn get_client_lease(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLease>> {
        self.observe("get_client_lease", || self.inner.get_client_lease(runtime))
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> Result<Option<ClientLease>> {
        self.observe("get_live_runtime_by_stable_id", || {
            self.inner.get_live_runtime_by_stable_id(stable_id)
        })
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        self.observe("list_live_clients", || self.inner.list_live_clients())
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        self.observe("publish_segment", || self.inner.publish_segment(segment))
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        self.observe("unpublish_segment", || {
            self.inner.unpublish_segment(owner, segment)
        })
    }

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<Option<SegmentAnnouncement>> {
        self.observe("get_segment", || self.inner.get_segment(owner, segment))
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        self.observe("list_segments", || self.inner.list_segments(owner))
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        self.observe("update_segment_state", || {
            self.inner.update_segment_state(owner, segment, next)
        })
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        self.observe("reserve_segment", || {
            self.inner.reserve_segment(owner, segment, length_bytes)
        })
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        self.observe("release_segment", || {
            self.inner
                .release_segment(owner, segment, offset_bytes, length_bytes)
        })
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        self.observe("get_object_route", || self.inner.get_object_route(key))
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        self.observe("list_object_routes", || self.inner.list_object_routes())
    }

    fn list_object_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        self.observe("list_object_routes_in_scope", || {
            self.inner.list_object_routes_in_scope(scope)
        })
    }

    fn get_object_route_by_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>> {
        self.observe("get_object_route_by_id", || {
            self.inner.get_object_route_by_id(object_id)
        })
    }

    fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        self.observe("list_reuse_candidates", || {
            self.inner.list_reuse_candidates(reuse)
        })
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.observe("compare_and_swap_object_route", || {
            self.inner
                .compare_and_swap_object_route(key, expected, next)
        })
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        self.observe("get_route_policy", || self.inner.get_route_policy(domain))
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        self.observe("put_route_policy_if_absent", || {
            self.inner.put_route_policy_if_absent(domain, policy)
        })
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        self.observe("put_route_policy", || {
            self.inner.put_route_policy(domain, policy)
        })
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        self.observe("delete_route_policy", || {
            self.inner.delete_route_policy(domain)
        })
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        self.observe("list_route_policies", || self.inner.list_route_policies())
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        self.observe("get_tenant_policy", || self.inner.get_tenant_policy(scope))
    }

    fn get_tenant_policies(
        &self,
        scopes: &[TenantPolicyScope],
    ) -> Result<Vec<Option<TenantPolicy>>> {
        self.observe("get_tenant_policies", || {
            self.inner.get_tenant_policies(scopes)
        })
    }

    fn list_tenant_policies(&self, tenant: Option<&str>) -> Result<Vec<TenantPolicy>> {
        self.observe("list_tenant_policies", || {
            self.inner.list_tenant_policies(tenant)
        })
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        self.observe("put_tenant_policy", || {
            self.inner.put_tenant_policy(policy, expected_version)
        })
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        self.observe("delete_tenant_policy", || {
            self.inner.delete_tenant_policy(scope, expected_version)
        })
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        self.observe("get_tenant_quota_state", || {
            self.inner.get_tenant_quota_state(scope)
        })
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        self.observe("get_tenant_object_accounting", || {
            self.inner.get_tenant_object_accounting(key)
        })
    }

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> Result<Option<TenantQuotaReservation>> {
        self.observe("get_tenant_quota_reservation", || {
            self.inner.get_tenant_quota_reservation(reservation_id)
        })
    }

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> Result<Vec<TenantObjectAccounting>> {
        self.observe("list_tenant_eviction_candidates", || {
            self.inner.list_tenant_eviction_candidates(scope, limit)
        })
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        self.observe("list_tenant_quota_reservations", || {
            self.inner.list_tenant_quota_reservations(scope)
        })
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        self.observe("reserve_tenant_quota", || {
            self.inner.reserve_tenant_quota(request)
        })
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        self.observe("finalize_tenant_quota", || {
            self.inner.finalize_tenant_quota(request)
        })
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        self.observe("abort_tenant_quota", || {
            self.inner.abort_tenant_quota(reservation_id)
        })
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        self.observe("put_handoff", || self.inner.put_handoff(handoff))
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        self.observe("get_handoff", || self.inner.get_handoff(stable_id))
    }
}
